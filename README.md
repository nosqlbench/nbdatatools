
# nbdatatools

This is an accessory module of the NoSQLBench project, focusing on test data management, 
particularly vector test data used for ANN testing.

Testing tools that require specialized data sometimes need an additional line of defense to ensure
that the data is appropriate. This repo is a place to put such tools in support of NoSQLBench and
other testing systems.


## modules

### specs

This documents the hdf5 layout standard used by this repo. The conventions described here are 
directly supported by the other modules.

The format used was initially inspired by
that of [ann-benchmark](https://github.com/erikbern/ann-benchmarks), but has since been extended
to support a significant variety of test data configurations.

### vectordata

This is an API for working directly with a test data format documented in this repo. This allows 
multiple testing systems to access the same data easily and consistently.  

### nbvectors

This is a collection of utilities for working with vector test data, hosted under a single 
command line tool. It is packaged as an executable jar with several sub-commands.

Here is a preview of the available commands:

    # see all the options
    # additional notes added to this view
    java -jar nbvectors.jar --help
    
    help            Display help information about the specified command.

    verify_knn      self-check KNN test data answer-keys
                    # not suggested for a full verification, but good for sparse
                    # sampling or basic smoke testing of the test data itself
                    # requires ranked gt metrics (of a known distance function)

    tag_hdf5        read or write hdf attributes
                    # this will likely be removed, as it will be obviated by others

    jjq             run jjq commands with extended functions
                    # runs jq-like expressions against streams of JSON data
                    # with added streaming analysis tools - useful for preparing
                    # external data for import into the standard test data format

    build_hdf5      build HDF5 KNN test data answer-keys from JSON
                    # takes hdf5 data from loosely structured JSON sources
                    # and builds a standard test data format
                    # !! This will likely be absorbed into export_hdf5 !!

    show_hdf5       show details of HDF5 KNN test data files
                    # This is useful to verify details of direct hdf5 structure
                    # or to see the logical model used for normalizing views
                    # of different data profiles or file contents

    export_hdf5     export HDF5 KNN answer-keys from other formats
                    # create a new hdf5 KNN test data file, from other formats,
                    # including ivec, fvec, bvec, and parquet

    export_json     export HDF5 KNN answer-keys to JSON summary files
                    # create a JSON summary file from an hdf5 KNN test data file

    catalog_hdf5    Create catalog views of HDF5 files
                    # This is useful for creating a catalog of hdf5 files
                    # that can be used to find and download specific files from
                    # a remote location

    hugging_dl      Download Huggingface Datasets via API
                    # This is useful for downloading datasets from Hugging Face
                    # any huggingface dataset which has a parquet format can be downloaded

    datasets        Browse and download hdf5 datasets from accessible catalogs
                    # This is useful for browsing and downloading datasets from accessible catalogs

    export_hdf5new  export HDF5 KNN answer-keys from other formats
                    # create a new hdf5 KNN test data file, from other formats
                    # this version supports layouts and embedded metadata which allow
                    # the vectordata API to work with the data in a logically consitent way
                    # !! This will likely be absorbed into export_hdf5 !!

Generally speaking, these utilities fall into a few broad categories, like import, export, 
summarization, and query. They will likely be consolidated down to a smaller number of tools over time.


## Java Version

This project is built with Java 23, and will tend to track the latest LTS at the very least. 
Generally speaking, one of the most effective ways to speed up your Java app is to use a modern 
JVM. The same applies to Java-based testing systems.

----

Ideally, users of these tools should have an experience like this:

* Consistent methods of finding documentation and getting CLI help
* Simple parameterization of commands and features
* User-friendly terminal output and status
* Basic quality-of-life features, like auto-completion and similar
