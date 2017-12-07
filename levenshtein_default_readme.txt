The associated script, levenshtein_default.py, is run to test whether the default Levenshtein distance ratio
cutoff is acceptable, given the claims that are being accepted and rejected. It does this by looking at the
claims that have been accepted via our algorithms (orcid_user and orcid_other) and comparing these to those that
have come from a credible source and are accepted automatically without using the default cutoff (orcid_pub) and
to those that have been rejected (determined by looking at the log files).

Theoretically, if the histograms of claims that have been accepted have many accepted claims near the cutoff,
particularly in orcid_pub, the cutoff may need to be lowered. Conversely, if there are few accepted claims near the
cutoff and many rejected claims near the cutoff, the cutoff may need to be raised. You can also look at the logs
for the rejected claims with a Levenshtein ratio near the cutoff, to ensure that claims that should clearly be rejected
are not too close to the cutoff; if there are, the cutoff should be raised. (Note that the cutoff shown on the
histograms is extracted from the ADSOrcid config.py; the actual cutoff may be set differently upon deployment.
Check the logs to see the cutoff being used currently.)

The script works by extracting data on accepted claims from SOLR and on rejected claims from log files. It queries
SOLR using the ORCID IDs of authors listed in the author table in postgres. You'll need to tunnel into both postgres
and SOLR first and then set the localhost paths (in local_config) in SQLALCHEMY_URL and SOLR_URL_OLD, respectively,
to be able to extract info on the accepted claims. To query the logs, grep for 'No match found' within the logs in the
docker container, then copy the output file locally. Save the output to a file 'logs_mismatch.txt' in the same folder
you want the output figures to be saved to.

To run the script:
> python levenshtein_default.py /path/to/output/folder

Logs reporting the code's status will be saved in the logs folder as 'levenshtein_test.log'. Note that the SOLR
queries can take some time to run.
