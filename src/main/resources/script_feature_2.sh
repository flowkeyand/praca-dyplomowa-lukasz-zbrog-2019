echo "Script Feature 2 Start...!"
hadoop fs -rm u.data
hadoop fs -rm -r feature_2
gsutil cp -r gs://dataproc-926b27f7-162c-44a3-913e-1ee0390d33eb-asia-northeast2/feature_2 .
cd feature_2
hadoop fs -put u.data
hadoop jar feature_2.jar u.data feature_2
echo "Script Feature 2 Finish Processing...!"
hadoop fs -cat ./feature_2/maxOut/*
hadoop fs -cat ./feature_2/historyOut/*
echo "----------------------------------------------"
echo "Script Feature 4 End...!"