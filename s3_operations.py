import re


def get_s3_bucket_objects_and_table_names(
    aws_session,
    bucket_name,
    s3_prefix=None,
    file_name_regex=None,  # another example is "(.+/)*{table_name}.csv",
):
    file_name_regex_ = None
    if file_name_regex:
        file_name_regex_ = "^" + file_name_regex.replace("{table_name}", "(?P<table_name>.+)") + "$"
        pattern = re.compile(file_name_regex_, re.IGNORECASE)

    s3_prefix_ = "" if s3_prefix is None else s3_prefix

    s3 = aws_session.resource("s3")
    bucket = s3.Bucket(bucket_name)

    s3_object_paths_and_file_names = []
    for s3_object in bucket.objects.filter(Prefix=s3_prefix_):
        file_full_path = s3_object.key
        if file_name_regex_ is None:
            file_name_without_extension = file_full_path.split("/")[-1].split(".")[0]
            s3_object_paths_and_file_names.append(
                (file_full_path, file_name_without_extension)
            )

        elif match := pattern.match(file_full_path):
            if "{table_name}" in file_name_regex:
                s3_object_paths_and_file_names.append(
                    (file_full_path, match.group("table_name"))
                )
            else:
                file_name_without_extension = file_full_path.split("/")[-1].split(".")[
                    0
                ]
                s3_object_paths_and_file_names.append(
                    (file_full_path, file_name_without_extension)
                )

    return s3_object_paths_and_file_names
