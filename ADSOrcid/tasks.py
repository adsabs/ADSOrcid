from __future__ import absolute_import, unicode_literals
import adsputils
from adsmsg import OrcidClaims
from ADSOrcid import app as app_module
from ADSOrcid import updater
from ADSOrcid.exceptions import ProcessingException, IgnorableException
from ADSOrcid.models import KeyValue
from kombu import Queue
import datetime
import os

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
app = app_module.ADSOrcidCelery(
    "orcid-pipeline",
    proj_home=proj_home,
    local_config=globals().get("local_config", {}),
)
app.conf.CELERY_QUEUES = (
    Queue("check-orcidid", app.exchange, routing_key="check-orcidid"),
    Queue("match-claim", app.exchange, routing_key="match-claim"),
    Queue("check-updates", app.exchange, routing_key="check-updates"),
    Queue("output-results", app.exchange, routing_key="output-results"),
)
logger = app.logger


# ============================= TASKS ============================================= #


@app.task(queue="check-orcidid")
def task_index_orcid_profile(message):
    """
    Fetch a fresh profile from the orcid-service and compare
    it against the state of the storage (diff). And re-index/update
    them.


    :param message: contains the message inside the packet
        {
         'orcidid': '.....',
         'start': 'ISO8801 formatted date (optional), indicates
             the moment we checked the orcid-service'
         'force': Boolean (if present, we'll not skip unchanged
             profile)
        }
    :return: no return
    """

    if "orcidid" not in message:
        raise IgnorableException("Received garbage: {}".format(message))

    message["start"] = adsputils.get_date()
    orcidid = message["orcidid"]
    author = app.retrieve_orcid(orcidid)

    # update profile table in microservice
    r = app.client.get(
        app.conf.get("API_ORCID_UPDATE_PROFILE") % orcidid,
        headers={
            "Accept": "application/json",
            "Authorization": "Bearer {0}".format(app.conf.get("API_TOKEN")),
        },
    )
    if r.status_code != 200:
        logger.warning("Profile for {0} not updated.".format(orcidid))

    orcid_present, updated, removed = app.get_claims(
        orcidid,
        app.conf.get("API_TOKEN"),
        app.conf.get("API_ORCID_EXPORT_PROFILE") % orcidid,
        force=message.get("force", False),
        orcid_identifiers_order=app.conf.get(
            "ORCID_IDENTIFIERS_ORDER", {"bibcode": 9, "*": -1}
        ),
    )

    to_claim = []

    # always insert a record that marks the beginning of a full-import
    # TODO: record orcid's last-modified-date
    to_claim.append(
        app.create_claim(
            bibcode="",
            orcidid=orcidid,
            provenance="OrcidImporter",
            status="#full-import",
            date=adsputils.get_date(),
        )
    )

    # find difference between what we have and what orcid has
    claims_we_have = set(updated.keys()).difference(set(removed.keys()))
    claims_orcid_has = set(orcid_present.keys())

    # those guys will be added (with ORCID date signature)
    for c in claims_orcid_has.difference(claims_we_have):
        claim = orcid_present[c]
        to_claim.append(
            app.create_claim(
                bibcode=claim[0],
                orcidid=orcidid,
                provenance=claim[2],
                status="claimed",
                date=claim[1],
            )
        )

    # those guys will be removed (since orcid doesn't have them)
    for c in claims_we_have.difference(claims_orcid_has):
        claim = updated[c]
        to_claim.append(
            app.create_claim(
                bibcode=claim[0],
                orcidid=orcidid,
                provenance="OrcidImporter",
                status="removed",
            )
        )

    # and those guys will be updated if their creation date is significantly off
    for c in claims_orcid_has.intersection(claims_we_have):
        orcid_claim = orcid_present[c]
        ads_claim = updated[c]

        delta = orcid_claim[1] - ads_claim[1]
        if delta.total_seconds() > app.conf.get("ORCID_UPDATE_WINDOW", 60):
            to_claim.append(
                app.create_claim(
                    bibcode=orcid_claim[0],
                    orcidid=orcidid,
                    provenance="OrcidImporter",
                    status="updated",
                    date=orcid_claim[1],
                )
            )
        elif message.get("force", False):
            to_claim.append(
                app.create_claim(
                    bibcode=orcid_claim[0],
                    orcidid=orcidid,
                    provenance="OrcidImporter",
                    status="forced",
                    date=orcid_claim[1],
                )
            )
        else:
            to_claim.append(
                app.create_claim(
                    bibcode=orcid_claim[0],
                    orcidid=orcidid,
                    provenance="OrcidImporter",
                    status="unchanged",
                    date=orcid_claim[1],
                )
            )

    if len(to_claim):
        # create record in the database
        json_claims = app.insert_claims(to_claim)
        if author["status"] in ("blacklisted", "postponed"):
            return
        # set to the queue for processing
        for claim in json_claims:
            if claim.get("bibcode"):
                claim["bibcode_verified"] = True
                claim["name"] = author["name"]
                if author.get("facts", None):
                    for k, v in author["facts"].items():
                        claim[k] = v

                claim["author_status"] = author["status"]
                claim["account_id"] = author["account_id"]
                claim["author_updated"] = author["updated"]
                claim["author_id"] = author["id"]

                if claim.get("status") != "removed":
                    claim["identifiers"] = orcid_present[
                        claim.get("bibcode").lower().strip()
                    ][3]
                    claim["author_list"] = orcid_present[
                        claim.get("bibcode").lower().strip()
                    ][4]

                task_match_claim.delay(claim)


@app.task(queue="match-claim")
def task_match_claim(claim, **kwargs):
    """
    Takes the claim, matches it in the database (will create
    entry for the record, if not existing yet) and updates
    the metadata.

    :param claim: contains the message inside the packet
        {'bibcode': '....',
        'orcidid': '.....',
        'name': 'author name',
        'facts': 'author name variants',
        }
    :return: no return
    """

    if not isinstance(claim, dict):
        raise ProcessingException("Received unknown payload {0}".format(claim))

    if not claim.get("orcidid"):
        raise ProcessingException("Unusable payload, missing orcidid {0}".format(claim))

    bibcode = claim["bibcode"]
    if claim.get("status") != "removed":
        identifiers = claim["identifiers"]
        authors = claim["author_list"]
    else:
        metadata = app.retrieve_metadata(bibcode)
        identifiers = metadata.get("identifier", [])
        authors = metadata.get("author", [])

    rec = app.retrieve_record(bibcode, authors)

    cl = updater.update_record(rec, claim, app.conf.get("MIN_LEVENSHTEIN_RATIO", 0.9))
    unique_bibs = list(set([bibcode] + identifiers))
    
    if cl:
        status = "verified"
        app.record_claims(bibcode, rec["claims"], rec["authors"])
        msg = OrcidClaims(
            authors=rec.get("authors"),
            bibcode=rec["bibcode"],
            verified=rec.get("claims", {}).get("verified", []),
            unverified=rec.get("claims", {}).get("unverified", []),
        )
        task_output_results.delay(msg)
    else:
        status = "rejected"
        logger.warning(
            "Claim refused for bibcode:{0} and orcidid:{1}".format(
                claim["bibcode"], claim["orcidid"]
            )
        )

    r = app.client.post(
        app.conf.get("API_ORCID_UPDATE_BIB_STATUS") % claim.get("orcidid"),
        json={"bibcodes": unique_bibs, "status": status},
        headers={"Authorization": "Bearer {0}".format(app.conf.get("API_TOKEN"))},
    )
    if r.status_code != 200:
        logger.warning(
            "IDs {ids} for {orcidid} not updated to: {status}".format(
                ids=unique_bibs, orcidid=claim.get("orcidid"), status=status
            )
        )
    elif len(r.json()) != 1:
        logger.warning(
            "Number of updated bibcodes ({0}) does not match input ({1}) for {2}".format(
                r.text, unique_bibs, claim.get("orcidid")
            )
        )


@app.task(queue="output-results")
def task_output_results(msg):
    """
    This worker will forward results to the outside
    exchange (typically an ADSImportPipeline) to be
    incorporated into the storage

    :param msg: contains the orcid claim with all
            information necessary for updating the
            database, mainly:

            {'bibcode': '....',
             'authors': [....],
             'claims': {
                 'verified': [....],
                 'unverified': [...]
             }
            }
    :type: adsmsg.OrcidClaims
    :return: no return
    """
    app.forward_message(msg)


@app.task(queue="check-updates")
def task_check_orcid_updates(msg):
    """Check the orcid microservice for updated orcid profiles.

    This function is somewhat complex
    we are trying to defend against multiple executions (assuming
    that there is many workers and each of them can receive its own
    signal to start processing).

    Basically, we'll only want to check for updated profiles once.
    The synchronization is done via a database. So the worker
    updates the 'last.check' timestamp immediately (and we
    'optimistically' hope that it will be enough to prevent clashes;
    well - even if that is not a strong guarantee, it wouldn't be
    a tragedy if a profile is checked twice...)

    Additional difficulty is time synchronization: the worker can
    be executed as often as you like, but it will refuse to do any
    work unless the time window between the checks is large enough.
    """

    with app.session_scope() as session:
        kv = session.query(KeyValue).filter_by(key="last.check").first()
        if kv is None:
            kv = KeyValue(
                key="last.check", value="1974-11-09T22:56:52.518001Z"
            )  # force update

        latest_point = adsputils.get_date(kv.value)  # RFC 3339 format
        now = adsputils.get_date()

        total_wait = app.conf.get("ORCID_CHECK_FOR_CHANGES", 60 * 5)  # default is 5min
        delta = now - latest_point

        if delta.total_seconds() < total_wait:
            # register our own execution in the future
            task_check_orcid_updates.apply_async(
                args=(msg,), countdown=(total_wait - delta.total_seconds()) + 1
            )
        else:
            logger.info("Checking for orcid updates")

            # increase the timestamp by one microsec and get new updates
            latest_point = latest_point + datetime.timedelta(microseconds=1)
            r = app.client.get(
                app.conf.get("API_ORCID_UPDATES_ENDPOINT") % latest_point.isoformat(),
                params={"fields": ["orcid_id", "updated", "created"]},
                headers={
                    "Authorization": "Bearer {0}".format(app.conf.get("API_TOKEN"))
                },
            )

            if r.status_code != 200:
                logger.error(
                    "Failed getting {0}\n{1}".format(
                        app.conf.get("API_ORCID_UPDATES_ENDPOINT") % kv.value, r.text
                    )
                )
                msg["errcount"] = msg.get("errcount", 0) + 1

                # schedule future execution offset by number of errors (rca: do exponential?)
                task_check_orcid_updates.apply_async(
                    args=(msg,), countdown=total_wait + total_wait * msg["errcount"]
                )
                return

            if r.text.strip() == "":
                return task_check_orcid_updates.apply_async(
                    args=(msg,), countdown=total_wait
                )

            data = r.json()

            if len(data) == 0:
                return task_check_orcid_updates.apply_async(
                    args=(msg,), countdown=total_wait
                )

            msg["errcount"] = 0  # success, we got data from the api, reset the counter

            # we received the data, immediately update the databaes (so that other processes don't
            # ask for the same starting date)
            # data should be ordered by date updated (but to be sure, let's check it); we'll save it
            # as latest 'check point'
            dates = [adsputils.get_date(x["updated"]) for x in data]
            dates = sorted(dates, reverse=True)

            kv.value = dates[0].isoformat()
            session.merge(kv)
            session.commit()

            for rec in data:
                payload = {
                    "orcidid": rec["orcid_id"],
                    "start": latest_point.isoformat(),
                }
                task_index_orcid_profile.delay(payload)

            # recheck again
            task_check_orcid_updates.apply_async(args=(msg,), countdown=total_wait)


if __name__ == "__main__":
    app.start()
