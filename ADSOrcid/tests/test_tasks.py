import sys
import os

from mock import patch, PropertyMock
import unittest
import pytest
import adsputils as utils
from ADSOrcid import app, tasks
from ADSOrcid.models import Base
from ADSOrcid.exceptions import ProcessingException


class TestWorkers(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = os.path.join(os.path.dirname(__file__), "../..")
        self._app = tasks.app
        self.app = app.ADSOrcidCelery(
            "test",
            local_config={"SQLALCHEMY_URL": "sqlite:///", "SQLALCHEMY_ECHO": False},
        )
        tasks.app = self.app  # monkey-path the app object

        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()
        tasks.app = self._app

    def test_task_index_orcid_profile(self):
        with patch.object(self.app, "retrieve_orcid") as retrieve_orcid, patch.object(
            tasks.app.client, "get"
        ) as get, patch.object(self.app, "get_claims") as get_claims, patch.object(
            self.app, "insert_claims"
        ) as insert_claims, patch.object(
            tasks.task_index_orcid_profile, "apply_async"
        ) as task_index_orcid_profile, patch.object(
            tasks.task_match_claim, "delay"
        ) as next_task:
            r = PropertyMock()
            data = {"bibcode": {"status": "some status", "title": "some title"}}
            r.text = str(data)
            r.json = lambda: data
            r.status_code = 200
            get.return_value = r

            get_claims.return_value = (
                {
                    "bibcode1": (
                        "Bibcode1",
                        utils.get_date("2017-01-01"),
                        "provenance",
                        ["id1", "id2"],
                        ["Stern, D K", "author two"],
                    ),
                    "bibcode2": (
                        "Bibcode2",
                        utils.get_date("2017-01-01"),
                        "provenance",
                        ["id1", "id2"],
                        ["author one", "Stern, D K"],
                    ),
                    "bibcode3": (
                        "Bibcode3",
                        utils.get_date("2017-01-01"),
                        "provenance",
                        ["id1", "id2"],
                        ["Stern, D K", "author two"],
                    ),
                },
                {
                    "bibcode1": ("Bibcode1", utils.get_date("2017-01-01")),
                    "bibcode4": (
                        "Bibcode4",
                        utils.get_date("2017-01-01"),
                    ),  # we have, but orcid no more
                },
                {
                    "bibcode2": ("Bibcode2", utils.get_date("2017-01-01")),
                },
            )
            insert_claims.return_value = [
                {
                    "status": "#full-import",
                    "bibcode": "",
                    "created": "2017-05-26T21:29:22.726506+00:00",
                    "provenance": "OrcidImporter",
                    "orcidid": "0000-0003-3041-2092",
                    "id": None,
                },
                {
                    "status": "claimed",
                    "bibcode": "Bibcode2",
                    "created": "2017-01-01T00:00:00+00:00",
                    "provenance": "provenance",
                    "orcidid": "0000-0003-3041-2092",
                    "id": None,
                },
                {
                    "status": "claimed",
                    "bibcode": "Bibcode3",
                    "created": "2017-01-01T00:00:00+00:00",
                    "provenance": "provenance",
                    "orcidid": "0000-0003-3041-2092",
                    "id": None,
                },
                {
                    "status": "removed",
                    "bibcode": "Bibcode4",
                    "created": "2017-05-26T21:29:22.728368+00:00",
                    "provenance": "OrcidImporter",
                    "orcidid": "0000-0003-3041-2092",
                    "id": None,
                },
                {
                    "status": "unchanged",
                    "bibcode": "Bibcode1",
                    "created": "2017-01-01T00:00:00+00:00",
                    "provenance": "OrcidImporter",
                    "orcidid": "0000-0003-3041-2092",
                    "id": None,
                },
            ]

            self.assertFalse(next_task.called)

            # check authors can be skipped
            retrieve_orcid.return_value = {
                "status": "blacklisted",
                "name": "Stern, D K",
                "facts": {
                    "author": ["Stern, D", "Stern, D K", "Stern, Daniel"],
                    "orcid_name": ["Stern, Daniel"],
                    "author_norm": ["Stern, D"],
                    "name": "Stern, D K",
                },
                "orcidid": "0000-0003-2686-9241",
                "id": 1,
                "account_id": None,
                "updated": utils.get_date("2017-01-01"),
            }

            tasks.task_index_orcid_profile({"orcidid": "0000-0003-3041-2092"})

            self.assertFalse(next_task.called)

            retrieve_orcid.return_value = {
                "status": None,
                "name": "Stern, D K",
                "facts": {
                    "author": ["Stern, D", "Stern, D K", "Stern, Daniel"],
                    "orcid_name": ["Stern, Daniel"],
                    "author_norm": ["Stern, D"],
                    "name": "Stern, D K",
                },
                "orcidid": "0000-0003-2686-9241",
                "id": 1,
                "account_id": None,
                "updated": utils.get_date("2017-01-01"),
            }

            tasks.task_index_orcid_profile({"orcidid": "0000-0003-3041-2092"})

            self.assertTrue(next_task.called)
            self.assertEqual(next_task.call_count, 4)

            self.assertEqual(
                sorted(
                    [(x.bibcode, x.status) for x in insert_claims.call_args[0][0]]
                ),
                sorted(
                    [
                        ("", "#full-import"),
                        ("Bibcode2", "claimed"),
                        ("Bibcode3", "claimed"),
                        ("Bibcode4", "removed"),
                        ("Bibcode1", "unchanged"),
                    ]
                ),
            )
            
            self.assertEqual(
                sorted(
                    [
                        (x[0][0]["bibcode"], x[0][0]["status"])
                        for x in next_task.call_args_list
                    ]
                ),
                sorted(
                    [
                        ("Bibcode2", "claimed"),
                        ("Bibcode3", "claimed"),
                        ("Bibcode4", "removed"),
                        ("Bibcode1", "unchanged"),
                    ]
                ),
            )
            
            self.assertEqual(
                (
                    next_task.call_args_list[0][0][0]["bibcode"],
                    next_task.call_args_list[0][0][0]["author_list"],
                ),
                ("Bibcode2", ["author one", "Stern, D K"]),
            )

            self.assertEqual(
                (
                    next_task.call_args_list[0][0][0]["bibcode"],
                    next_task.call_args_list[0][0][0]["identifiers"],
                ),
                ("Bibcode2", ["id1", "id2"]),
            )

    def test_match_claim_unknown_payload_should_return_warning(self):
        with pytest.raises(ProcessingException) as exception_info:
            tasks.task_match_claim([])

        self.assertEqual(str(exception_info.value), "Received unknown payload []")

    def test_match_claim_unusable_payload_should_return_warning(self):
        claim = {
            "status": "claimed",
            "bibcode": "BIBCODE22",
            "name": "Stern, D K",
            "provenance": "provenance",
            "identifiers": ["id1", "id2"],
            "orcid_name": ["Stern, Daniel"],
            "author_norm": ["Stern, D"],
            "author_status": None,
            "author": ["Stern, D", "Stern, D K", "Stern, Daniel"],
            "author_id": 1,
            "account_id": None,
            "author_list": ["Stern, D K", "author two"],
        }
        with pytest.raises(ProcessingException) as exception_info:
            tasks.task_match_claim(claim)
        self.assertEqual(
            str(exception_info.value),
            "Unusable payload, missing orcidid {0}".format(claim),
        )

    def test_task_match_claim_cl_status_200_should_return_correct_message(self):
        with patch.object(self.app, "retrieve_record") as retrieve_record, patch.object(
            self.app, "record_claims"
        ) as record_claims, patch.object(
            tasks.app.client, "post"
        ) as post, patch.object(
            tasks.task_output_results, "delay"
        ) as next_task:
            retrieve_record.return_value = {
                "bibcode": "BIBCODE22",
                "authors": ["Einstein, A", "Socrates", "Stern, D K", "Munger, C"],
                "claims": {
                    "verified": ["-", "-", "-", "-"],
                    "unverified": ["-", "-", "-", "-"],
                },
            }

            r = PropertyMock()
            data = {"BIBCODE22": "status"}
            r.text = str(data)
            r.json = lambda: data
            r.status_code = 200
            post.return_value = r

            self.assertFalse(next_task.called)
            tasks.task_match_claim(
                {
                    "status": "claimed",
                    "bibcode": "BIBCODE22",
                    "name": "Stern, D K",
                    "provenance": "provenance",
                    "identifiers": ["id1", "id2"],
                    "orcid_name": ["Stern, Daniel"],
                    "author_norm": ["Stern, D"],
                    "author_status": None,
                    "orcidid": "0000-0003-3041-2092",
                    "author": ["Stern, D", "Stern, D K", "Stern, Daniel"],
                    "author_id": 1,
                    "account_id": None,
                    "author_list": ["Stern, D K", "author two"],
                }
            )

            self.assertEqual(
                (
                    "BIBCODE22",
                    {
                        "verified": ["-", "-", "-", "-"],
                        "unverified": ["-", "-", "0000-0003-3041-2092", "-"],
                    },
                    ["Einstein, A", "Socrates", "Stern, D K", "Munger, C"],
                ),
                record_claims.call_args[0],
            )

            self.assertEqual(
                {
                    "bibcode": "BIBCODE22",
                    "authors": ["Einstein, A", "Socrates", "Stern, D K", "Munger, C"],
                    "verified": ["-", "-", "-", "-"],
                    "unverified": ["-", "-", "0000-0003-3041-2092", "-"],
                },
                next_task.call_args[0][0].toJSON(),
            )

    def test_task_match_claim_no_cl_should_not_call_record_claims(self):
        with patch.object(self.app, "retrieve_record") as retrieve_record, patch.object(
            self.app, "record_claims"
        ) as record_claims, patch(
            "ADSOrcid.updater.update_record"
        ) as mock_update, patch.object(
            tasks.app.client, "post"
        ) as post, patch.object(
            tasks.task_output_results, "delay"
        ) as next_task:
            retrieve_record.return_value = {
                "bibcode": "BIBCODE22",
                "authors": ["Einstein, A", "Socrates", "Stern, D K", "Munger, C"],
                "claims": {
                    "verified": ["-", "-", "-", "-"],
                    "unverified": ["-", "-", "-", "-"],
                },
            }
            mock_update.return_value = None
            r = PropertyMock()
            data = {"BIBCODE22": "status"}
            r.text = str(data)
            r.json = lambda: data
            r.status_code = 200
            post.return_value = r

            self.assertFalse(next_task.called)
            tasks.task_match_claim(
                {
                    "status": "claimed",
                    "bibcode": "BIBCODE22",
                    "name": "Stern, D K",
                    "provenance": "provenance",
                    "identifiers": ["id1", "id2"],
                    "orcid_name": ["Stern, Daniel"],
                    "author_norm": ["Stern, D"],
                    "author_status": None,
                    "orcidid": "0000-0003-3041-2092",
                    "author": ["Stern, D", "Stern, D K", "Stern, Daniel"],
                    "author_id": 1,
                    "account_id": None,
                    "author_list": ["Stern, D K", "author two"],
                }
            )
            record_claims.assert_not_called()
            next_task.assert_not_called()

    def test_logger_warning_task_match_claim_no_cl_should_be_refused(self):
        with patch("logging.Logger.warning") as mock_warning, patch(
            "ADSOrcid.updater.update_record"
        ) as mock_update, patch.object(tasks.app.client, "post") as post:
            mock_update.return_value = None

            r = PropertyMock()
            data = {"BIBCODE22": "status"}
            r.text = str(data)
            r.json = lambda: data
            r.status_code = 200
            post.return_value = r

            tasks.task_match_claim(
                claim={
                    "status": "claimed",
                    "bibcode": "BIBCODE22",
                    "name": "Stern, D K",
                    "provenance": "provenance",
                    "identifiers": ["id1", "id2"],
                    "orcid_name": ["Stern, Daniel"],
                    "author_norm": ["Stern, D"],
                    "author_status": None,
                    "orcidid": "0000-0003-3041-2092",
                    "author": ["Stern, D", "Stern, D K", "Stern, Daniel"],
                    "author_id": 1,
                    "account_id": None,
                    "author_list": ["Stern, D K", "author two"],
                }
            )

            mock_warning.assert_called_with(
                "Claim refused for bibcode:BIBCODE22 and orcidid:0000-0003-3041-2092"
            )

    def test_task_match_claim_warning_no_cl_status_code_not_200_should_return_not_updated_to_rejected(
        self,
    ):
        with patch("logging.Logger.warning") as mock_warning, patch(
            "ADSOrcid.updater.update_record"
        ) as mock_update, patch.object(tasks.app.client, "post") as post:
            mock_update.return_value = None
            r = PropertyMock()
            data = {"BIBCODE22": "status"}
            r.text = str(data)
            r.json = lambda: data
            r.status_code = 404
            post.return_value = r

            tasks.task_match_claim(
                claim={
                    "status": "claimed",
                    "bibcode": "BIBCODE22",
                    "name": "Stern, D K",
                    "provenance": "provenance",
                    "identifiers": ["id1", "id2"],
                    "orcid_name": ["Stern, Daniel"],
                    "author_norm": ["Stern, D"],
                    "author_status": None,
                    "orcidid": "0000-0003-3041-2092",
                    "author": ["Stern, D", "Stern, D K", "Stern, Daniel"],
                    "author_id": 1,
                    "account_id": None,
                    "author_list": ["Stern, D K", "author two"],
                }
            )

            warning_args = mock_warning.call_args

            self.assertIn("id1", warning_args[0][0])
            self.assertIn("id2", warning_args[0][0])
            self.assertIn("BIBCODE22", warning_args[0][0])
            self.assertIn("0000-0003-3041-2092", warning_args[0][0])
            self.assertIn("rejected", warning_args[0][0])

    def test_task_match_claim_warning_cl_status_code_not_200_should_return_not_updated_to_verified(
        self,
    ):
        with patch("logging.Logger.warning") as mock_warning, patch(
            "ADSOrcid.updater.update_record"
        ) as mock_update, patch.object(
            self.app, "record_claims"
        ) as record_claims, patch.object(
            tasks.app.client, "post"
        ) as post, patch.object(
            tasks.task_output_results, "delay"
        ) as next_task:
            r = PropertyMock()
            data = {"BIBCODE22": "status"}
            r.text = str(data)
            r.json = lambda: data
            r.status_code = 404
            post.return_value = r

            tasks.task_match_claim(
                claim={
                    "status": "claimed",
                    "bibcode": "BIBCODE22",
                    "name": "Stern, D K",
                    "provenance": "provenance",
                    "identifiers": ["id1", "id2"],
                    "orcid_name": ["Stern, Daniel"],
                    "author_norm": ["Stern, D"],
                    "author_status": None,
                    "orcidid": "0000-0003-3041-2092",
                    "author": ["Stern, D", "Stern, D K", "Stern, Daniel"],
                    "author_id": 1,
                    "account_id": None,
                    "author_list": ["Stern, D K", "author two"],
                }
            )

            warning_args = mock_warning.call_args

            self.assertIn("id1", warning_args[0][0])
            self.assertIn("id2", warning_args[0][0])
            self.assertIn("BIBCODE22", warning_args[0][0])
            self.assertIn("0000-0003-3041-2092", warning_args[0][0])
            self.assertIn("not updated to", warning_args[0][0])
            self.assertIn("verified", warning_args[0][0])

            self.assertTrue(record_claims.called)
            self.assertTrue(next_task.called)

    def test_task_match_claim_warning_cl_bibcode_length_is_different_should_return_does_not_match(
        self,
    ):
        with patch("logging.Logger.warning") as mock_warning, patch(
            "ADSOrcid.updater.update_record"
        ) as mock_update, patch.object(
            self.app, "record_claims"
        ) as record_claims, patch.object(
            tasks.app.client, "post"
        ) as post, patch.object(
            tasks.task_output_results, "delay"
        ) as next_task:
            r = PropertyMock()
            data = {"BIBCODE22": "status"}
            r.text = str(data)
            r.json.return_value = {}
            r.status_code = 200
            post.return_value = r

            tasks.task_match_claim(
                claim={
                    "status": "claimed",
                    "bibcode": "BIBCODE22",
                    "name": "Stern, D K",
                    "provenance": "provenance",
                    "identifiers": ["id1", "id2"],
                    "orcid_name": ["Stern, Daniel"],
                    "author_norm": ["Stern, D"],
                    "author_status": None,
                    "orcidid": "0000-0003-3041-2092",
                    "author": ["Stern, D", "Stern, D K", "Stern, Daniel"],
                    "author_id": 1,
                    "account_id": None,
                    "author_list": ["Stern, D K", "author two"],
                }
            )

            warning_args = mock_warning.call_args

            self.assertIn("id1", warning_args[0][0])
            self.assertIn("id2", warning_args[0][0])
            self.assertIn("BIBCODE22", warning_args[0][0])
            self.assertIn("0000-0003-3041-2092", warning_args[0][0])
            self.assertIn("does not match input", warning_args[0][0])

            self.assertTrue(record_claims.called)
            self.assertTrue(next_task.called)

    def test_task_match_removed_claim(self):
        with patch.object(self.app, "retrieve_record") as retrieve_record, patch.object(
            self.app, "retrieve_metadata"
        ) as retrieve_metadata, patch.object(
            self.app, "record_claims"
        ) as record_claims, patch.object(
            tasks.app.client, "post"
        ) as post, patch.object(
            tasks.task_output_results, "delay"
        ) as next_task:
            retrieve_record.return_value = {
                "bibcode": "BIBCODE22",
                "authors": ["Einstein, A", "Socrates", "Stern, D K", "Munger, C"],
                "claims": {
                    "verified": ["-", "-", "-", "-"],
                    "unverified": ["-", "-", "0000-0003-3041-2092", "-"],
                },
            }

            retrieve_metadata.return_value = {
                "identifier": ["id1", "id2"],
                "author_list": [
                    "author one",
                    "author two",
                    "Stern, D K",
                    "author four",
                ],
            }

            r = PropertyMock()
            data = {"BIBCODE22": "status"}
            r.text = str(data)
            r.json = lambda: data
            r.status_code = 200
            post.return_value = r

            self.assertFalse(next_task.called)
            tasks.task_match_claim(
                {
                    "status": "removed",
                    "bibcode": "BIBCODE22",
                    "name": "Stern, D K",
                    "provenance": "provenance",
                    "orcid_name": ["Stern, Daniel"],
                    "author_norm": ["Stern, D"],
                    "author_status": None,
                    "orcidid": "0000-0003-3041-2092",
                    "author": ["Stern, D", "Stern, D K", "Stern, Daniel"],
                    "author_id": 1,
                    "account_id": None,
                }
            )

            self.assertEqual(
                (
                    "BIBCODE22",
                    {
                        "verified": ["-", "-", "-", "-"],
                        "unverified": ["-", "-", "-", "-"],
                    },
                    ["Einstein, A", "Socrates", "Stern, D K", "Munger, C"],
                ),
                record_claims.call_args[0],
            )

            self.assertEqual(
                {
                    "bibcode": "BIBCODE22",
                    "authors": ["Einstein, A", "Socrates", "Stern, D K", "Munger, C"],
                    "verified": ["-", "-", "-", "-"],
                    "unverified": ["-", "-", "-", "-"],
                },
                next_task.call_args[0][0].toJSON(),
            )

    def test_task_check_orcid_updates(self):
        with patch.object(tasks.app.client, "get") as get, patch.object(
            tasks.task_index_orcid_profile, "delay"
        ) as next_task, patch.object(
            tasks.task_check_orcid_updates, "apply_async"
        ) as recheck_task:
            # data = open(os.path.join(self.proj_home, 'ADSOrcid/tests/stub_data', '0000-0003-3041-2092.orcid-updates.json'), 'r').read()
            data = [
                {"orcid_id": "0000-0003-3041-2092", "updated": str(utils.get_date())},
                {"orcid_id": "0000-0003-3041-2093", "updated": str(utils.get_date())},
            ]
            r = PropertyMock()
            r.text = str(data)
            r.json = lambda: data
            r.status_code = 200
            get.return_value = r

            tasks.task_check_orcid_updates({})  # could be anything

            self.assertEqual(
                next_task.call_args_list[0][0][0]["orcidid"], "0000-0003-3041-2092"
            )
            self.assertEqual(
                next_task.call_args_list[1][0][0]["orcidid"], "0000-0003-3041-2093"
            )
            self.assertEqual(
                str(recheck_task.call_args_list[0]),
                "call(args=({'errcount': 0},), countdown=300)",
            )
           


if __name__ == "__main__":
    unittest.main()
