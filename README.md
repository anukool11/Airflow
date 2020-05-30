These scripts assume that you have setup the airflow.cfg file to trigger email alerts when a task succeeds or fails. In case one hasn't, the following steps must be followed:
  1. Open the airflow.cfg file. 
  2. Go to the [smtp] section
  3. Set host_name is smtp.gmail.com
  4. Set port as 587
  5. Provide your email address and the app password  
