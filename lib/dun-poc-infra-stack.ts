import * as lambda from '@aws-cdk/aws-lambda';
import * as apigw from '@aws-cdk/aws-apigateway';
import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as iam from '@aws-cdk/aws-iam';
import * as tf from '@aws-cdk/aws-transfer';

export class DunPocInfraStack extends cdk.Stack {
  constructor(scope: cdk.App, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const upload = new lambda.Function(this, 'UploadS3Handler', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda'), 
      handler: 'upload.handler'                
    });

    const httpApi = new apigw.RestApi(this, 'uploadApi', {
      deployOptions: {
        stageName: "dev",
        tracingEnabled: true,
        cachingEnabled: true,
        loggingLevel: apigw.MethodLoggingLevel.INFO,
        metricsEnabled: true
      },
    });

    const uploadIntegration = new apigw.LambdaIntegration(upload,);

    const uploadProxy = httpApi.root.addResource("upload");

    uploadProxy.addMethod(
      "POST",
      uploadIntegration,
      { 
        methodResponses: [{ statusCode: "200" }, { statusCode: "500" }],
        authorizationType: apigw.AuthorizationType.IAM
      }
    );

    const bucket = new s3.Bucket(this, 'UploadBucket', {
      bucketName: 'dunbradstreet-poc-' + this.region + '-' + this.account,
      encryption: s3.BucketEncryption.S3_MANAGED,
      versioned: true
    });

    const role = new iam.Role(this, 'AWSTransferFamilyServiceRole', {
      roleName: 'AWSTransferLoggingAccess',
      assumedBy: new iam.ServicePrincipal('transfer.amazonaws.com'),
    });

    const sftpUpload = new lambda.Function(this, 'UploadSFTPHandler', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda'), 
      handler: 'sftpUpload.handler'                
    });

    const lambdaPolicy = new iam.PolicyStatement({
      actions: ['transfer:SendWorkflowStepState',
                's3:*'
              ],
      resources: ['*'],
    });

    sftpUpload.role?.attachInlinePolicy(
      new iam.Policy(this, 'lambda-inlinepolicy', {
      statements: [lambdaPolicy],
    }),);

    role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSTransferLoggingAccess'));
    role.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['lambda:InvokeFunction', 'lambda:InvokeAsync'],
        resources: [sftpUpload.functionArn],
      })
    )

    const userRole = new iam.Role(this, 'AWSTransferUsersAccessRole', {
      roleName: 'AWSTransferUsersAccess',
      assumedBy: new iam.ServicePrincipal("transfer.amazonaws.com"),
    });
  
    userRole.addToPolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: ['s3:ListBucket'],
          resources: [bucket.bucketArn],
        })
    );
  
    userRole.addToPolicy(
      new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
              's3:GetObject',
              's3:GetObjectAcl',
              's3:GetObjectVersion',
              's3:PutObject',
              's3:PutObjectACL',
              's3:DeleteObject',
              's3:DeleteObjectVersion'
          ],
          resources: [`${bucket.bucketArn}/*`],
      })
    );

    const server = new tf.CfnServer(this, 'TransferFamilyServer', {
      protocols: ['SFTP'],
      identityProviderType: 'SERVICE_MANAGED',
      loggingRole: role.roleArn,
    });
  }
}
