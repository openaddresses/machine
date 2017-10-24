import argparse
import boto3
import os
import psycopg2
import psycopg2.extras


def http_url_to_s3(http_url):
    """ Convert an https://s3.amazonaws.com... url into a bucket/key tuple """
    (bucket, key) = http_url[25:].split('/', 1)
    return (bucket, key)


def delete_url(s3, http_url):
    if not http_url:
        return

    b, k = http_url_to_s3(http_url)
    print("Deleting S3 object at {}".format(http_url))
    s3.Object(b, k).delete()


def place_deletion_readme(s3, destination, reference_url):
    text = ("Some of this run's data was removed at the request of a provider.\n\n"
            "For more information, see: {}").format(reference_url)
    b, k = http_url_to_s3(destination)
    s3.Object(b, k).put(
        Body=text.encode('utf8'),
        ContentType='text/plain; charset=utf-8',
    )
    print("Created a README at s3://{}/{}".format(b, k))


def delete_source(conn, source_path, reference_url):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT state->'run id' as run_id,
               state->'cache' as cache,
               state->'processed' as processed,
               state->'sample' as sample,
               state->'slippymap' as slippymap
        FROM runs
        WHERE source_path = %s
          AND copy_of IS NULL
        """, (source_path,)
    )

    assert cur.rowcount > 0, "No runs found for this source path"

    s3 = boto3.resource('s3')

    for row in cur:
        delete_url(s3, row['cache'])
        delete_url(s3, row['processed'])
        delete_url(s3, row['sample'])
        delete_url(s3, row['slippymap'])

        # Place a marker explaining that data was removed
        readme_destination = row['cache'].replace('cache.zip', 'README.redaction')
        place_deletion_readme(s3, readme_destination, reference_url)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'source',
        help='The source path to remove e.g. sources/us/il/statewide.json'
    )
    parser.add_argument(
        'reference',
        help='An (e.g. GitHub issue) URL to point to in the readme that explains the data removal'
    )

    args = parser.parse_args()

    database_url = os.environ.get('DATABASE_URL')
    assert database_url, "Specify database url on DATABASE_URL environment variable"

    conn = psycopg2.connect(database_url)
    delete_source(conn, args.source, args.reference)
