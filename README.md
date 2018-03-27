# Insight API

Insight is SciDB's R API to enable clinical trial analytics at scale.

The API leverages the multidimensional array database [SciDB](https://www.paradigm4.com/) to handle:

- multiple data sources
- multiple formats
- multiple data-types

Other features of the API include

- A **universal yet flexible schema** inspired by the recommendations of the GA4GH (global alliance for genomics and health) action group. 
- A **uniform API** for loeading and querying diverse data
- Other industry-standard requirements like
    + **study-level security**
    + ability to store **different release versions**
    + **scalablility** to handle TB-s of data

Get more details at [the API documentation page](https://paradigm4.github.io/insight-docs/)

# R package and installation

This repository provides an R package for the SciDB Insight API. You can install this using `devtools`

```sh
# requires the devtools package
R --slave -e "devtools::install_github('paradigm4/insight')
```
