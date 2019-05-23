echo "Script Feature 1 Start...!"
hadoop fs -rm u.data
hadoop fs -rm -r feature_1
gsutil cp -r gs://dataproc-926b27f7-162c-44a3-913e-1ee0390d33eb-asia-northeast2/feature_1 .
cd feature_1
hadoop fs -put u.data
hadoop jar feature_1.jar u.data feature_1
echo "Script Feature 1 Finish Processing...!"
hadoop fs -cat ./feature_1/basic1Out/*
hadoop fs -cat ./feature_1/basic2Out/*
hadoop fs -cat ./feature_1/advOut/*
echo "---------------------------------"
echo "Script Feature 1 End...!"