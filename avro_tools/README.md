# Avro Schema Validation Tool

This script provides an easy way to validate Avro schema definition files.

### Running the tool

It's possible to run the validation on a single file or to recursively search for files in a folder and validate all of them.
Please use absolute paths.

```bash
$ python ./avro_tools/validation.py -p <file_or_folder_path>
STARTING...
SUCCESS: AVRO schema in file  <file_name_1> is valid.
SUCCESS: AVRO schema in file  <file_name_2> is valid.
...
ALL DONE!
```

When scanning an entire folder, it's also possible to pass an expression to restrict the files to be validated.

```bash
$ python ./avro_tools/validation.py -p <file_or_folder_path> -f foobar
STARTING...
SUCCESS: AVRO schema in file  <file_name_containing_foobar> is valid.
...
ALL DONE!
```

The flag `-q` can be passed to print out only the files that fail the validation.

This information can also be seen by running

```bash
$ python ./avro_tools/validation.py -h
```