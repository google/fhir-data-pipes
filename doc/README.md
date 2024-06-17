# Documentations

This directory contains various documentation pages. Here is a short summary of
what each file and/or directory is for and how to use them:

* [developers.md](developers.md) contains some tips for developers.
* [review_process.md](review_process.md) describes our review process. Before
  sending a PR for review, please make sure you understand and follow this.
* [schema.md](schema.md) describes the Parquet schema for projecting FHIR
  resources. The two files [Patient_US-Core.schema](Patient_US-Core.schema) and
  [Patient_no-extension.schema](Patient_no-extension.schema) are example schema
  files that are referenced from [schema.md](schema.md).
* [openhim.md](openhim.md) explains how to integrate the pipelines with OpenHIM.

The rest of the files/directories are used for generating our documentation site
and are described below.

## Documentation site

The [docs/](docs) directory contains the content for building our documentation
site at []().
To make changes to these documentations you can follow
[MkDocs](https://www.mkdocs.org/getting-started/) steps, namely:
* Create a Python virtual env.
  ```shell
  virtualenv -p python3 venv
  source ./venv/bin/activate
  ```
* Install the requirements:
  ```shell
  pip install -r requirements.txt
  ```
* To see local changes, run the dev server and check:
  [http://127.0.0.1:8000/analytics](http://127.0.0.1:8000/analytics)
  ```shell
  mkdocs serve
  ```
* To build the pages:
  ```shell
  mkdocs build
  ```
* To [deploy](https://www.mkdocs.org/user-guide/deploying-your-docs/) the pages:
  ```shell
  mkdocs gh-deploy
  ```
* Do _not_ commit the generated `sites/` directory to the `master` branch. The
  docs website at []() is generated from the `gh-pages` branch; so `sites/`
  updates only go to this branch.
