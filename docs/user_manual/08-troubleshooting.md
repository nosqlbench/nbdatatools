# Troubleshooting

Common issues and resolutions when working with NBDataTools dataset directories.

## Memory Issues

### OutOfMemoryError: Java heap space
```
Exception in thread "main" java.lang.OutOfMemoryError: Java heap space
```
- Increase heap size: `java -Xmx8g -jar nbvectors.jar analyze describe datasets/mteb-lite`
- Prebuffer only required profiles: `java -jar nbvectors.jar datasets prebuffer datasets/mteb-lite --profile default`
- Sample instead of full scans: `... analyze describe datasets/mteb-lite --sample-size 10000`

### OutOfMemoryError: Direct buffer memory
```
java.lang.OutOfMemoryError: Direct buffer memory
```
- Increase direct memory: `java -XX:MaxDirectMemorySize=2g -jar nbvectors.jar ...`

## Conversion Issues

### Unsupported format / invalid header
- Specify formats explicitly: `java -jar nbvectors.jar convert file --input vectors.dat --input-format fvec --output vectors.csv`
- Verify file headers with `hexdump -C file | head -5`
- Force dimensions: `... convert file --dimensions 128`

## Catalog Issues

### Datasets missing in catalog output
- Ensure each directory contains `dataset.yaml`
- Use recursive mode: `java -jar nbvectors.jar catalog --directories ./datasets --recursive`
- Include stats/checksums: `... --include-statistics --include-checksums`

## Dataset Manifest Errors

### "dataset.yaml not found"
- Confirm path is correct and case-sensitive
- Use absolute paths or `"~/datasets/my-data"`

### "Invalid profile" / "unknown facet"
- Check `profiles` section in `dataset.yaml`
- Verify `source` paths reference existing `.fvec/.ivec/.bvec` files

## Download Problems

### Slow downloads
- Increase parallelism: `... datasets download dataset:default --threads 4`
- Use `--resume` for interrupted transfers

### Corrupt or incomplete facets
- Verify with Merkle: `java -jar nbvectors.jar merkle verify --file datasets/mteb-lite/base.fvec --reference base.mref`
- Re-download with `--force`

## CLI Debugging

- Enable debug logs: `java -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -jar nbvectors.jar ...`
- Log GC activity: `java -XX:+PrintGCDetails -Xloggc:gc.log -jar nbvectors.jar ...`

## Network Issues

- Trace HTTP requests: `java -Dcom.sun.net.httpserver.HttpServer.debug=true -jar nbvectors.jar datasets download dataset:default`
- Force HTTPS: `java -Dnbdatatools.http.ssl_only=true -jar nbvectors.jar ...`

## Integrity Verification

- Create Merkle references per facet: `java -jar nbvectors.jar merkle create --file base.fvec --output base.mref`
- Verify later with `merkle verify`
- Check checksums provided by catalogs (`datasets download --verify`)

## When All Else Fails

- Run `java -jar nbvectors.jar --version` and ensure you are on the latest release.
- Re-run commands with `--debug` and capture logs.
- File issues with command output, logs, and `dataset.yaml` snippets for faster triage.
