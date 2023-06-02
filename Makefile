.PHONY: sync-testdata

sync-testdata:
	aws s3 sync --delete testdata/fixture/ s3://${TEST_BUCKET_NAME}/${TEST_OBJECT_PATH_PREFIX}
