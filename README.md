DynamoDB Stream to ElasticSearch Lambda
=================

### Author: Vladimir Budilov
* [YouTube](https://www.youtube.com/channel/UCBl-ENwdTlUsLY05yGgXyxw)
* [LinkedIn](https://www.linkedin.com/in/vbudilov/)
* [Medium](https://medium.com/@budilov)


### Why? 

Because I didn't want to re-invent the wheel every time, from scratch. This is a templated project that will let you get started quickly

### What does it do?

Simple -- it copies your DDB Stream data to ES. That's it. It assumes a lot and you will need to tweak it if you want to save anything but Strings, but it's easy to add that logic later on

### How? 
These 2 Parameter Store variables need to exist in order for the Lambda function to function properly. The first specifies which DDB Stream to attach to and the second specifies which ElasticSearch endpoint to propagate the data to.
```yaml
      arn: ${ssm:/ccUsersTableStreamArn}
      esUrl: ${ssm:/ccEsDomainEndpoint}
```

### Easy deployment? 
Yes. I'm using the Serverless framework to deploy the Lambda function to AWS and here's what you need to run:

```shell script
./gradlew clean build && ./gradlew deploy
```

### What's next? 

Enjoy
