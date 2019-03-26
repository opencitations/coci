# COCI (The OpenCitations Index of Crossref open DOI-to-DOI citations)
COCI is the first open DOI-to-DOI citations index provided by [OpenCitations](http://opencitations.net). It contains almost 450 million citation links coming from both the ‘Open’ and the ‘Limited’ sets of Crossref reference data. We have applied the concept of citations as first-class data entities. New data model schemes have been added to the OpenCitations Data Model (OCDM), basically driven from the introduction of the new ‘Citation’ class.

OpenCitations offers several options to access/query the data of COCI:
1) SPARQL endpoint (https://w3id.org/oc/index/coci/sparql): we offer a SPARQL endpoint editor GUI. This option is suitable for users who know how to formulate SPARQL queries. This option is available in the OpenCitations website .
2) The COCI REST API (https://w3id.org/oc/index/coci/api/v1): implemented using [RAMOSE (the Restful API Manager Over SPARQL Endpoints)](https://github.com/opencitations/ramose).

3) The Searching/Browsing Interface (http://opencitations.net/index/search): a user-friendly search interface which could be used to search inside the COCI dataset, while hiding the actual SPARQL queries, thus making the search operations accessible to those who are not experts in Semantic Web technologies. These interfaces have been implemented using [OSCAR](https://github.com/opencitations/oscar) and [LUCINDA](https://github.com/opencitations/lucinda).

4) Data dumps (http://opencitations.net/download#coci): COCI data are available and hosted as dumps on the Figshare service in CSV and N-Triples formats, while the dump of the whole triplestore is available on The Internet Archive.


## COCI Workflow
COCI has been created following the scheme in the figure bellow. All the related scripts could be found in the `script/` folder of this repository.  

![chart_flow](https://ivanhb.github.io/phd/paper/coci_iswc2019/img/grflow(prov).png)
