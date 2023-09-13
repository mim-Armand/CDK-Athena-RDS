import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Stack, StackProps, SecretValue } from 'aws-cdk-lib';
import { CfnNamedQuery } from 'aws-cdk-lib/aws-athena';
import { CfnDatabase, CfnCrawler } from 'aws-cdk-lib/aws-glue';
import { Secret } from 'aws-cdk-lib/aws-secretsmanager';
import { CfnOutput } from 'aws-cdk-lib';
import {Role, ServicePrincipal, PolicyStatement, Effect, ManagedPolicy} from 'aws-cdk-lib/aws-iam';
import { Function, Runtime, Code } from 'aws-cdk-lib/aws-lambda';
// import { AwsSdkCall } from 'aws-cdk-lib/aws-stepfunctions-tasks';



class AthenaRDSQueryStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const gluCrawlerName = 'pocGlueCrawler';

    // Retrieve RDS credentials from Secrets Manager
    const rdsCredentialsSecret = Secret.fromSecretCompleteArn(this, 'RdsCredentials', `arn:aws:secretsmanager:${this.region}:${this.account}:secret:DBSecretD58955BC-Aarz2ser4gmV-bum8C4`);


    // Create an IAM role for the Lambda function with necessary permissions
    const lambdaExecutionRole = new Role(this, 'LambdaExecutionRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
        ManagedPolicy.fromAwsManagedPolicyName('AmazonRDSReadOnlyAccess'),
        ManagedPolicy.fromAwsManagedPolicyName('SecretsManagerReadWrite'),
      ]
    });
    // const lambdaExecutionRole = new Role(this, 'LambdaExecutionRole', {
    //   assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
    // });
    //
    // lambdaExecutionRole.addToPolicy(new PolicyStatement({
    //   actions: [
    //     'logs:CreateLogGroup',
    //     'logs:CreateLogStream',
    //     'logs:PutLogEvents',
    //     'secretsmanager:GetSecretValue',
    //     'glue:GetDatabases',
    //     'glue:GetTables',
    //     'athena:StartQueryExecution',
    //     'athena:GetQueryResults',
    //     's3:PutObject',
    //     's3:GetObject',
    //     'rds:DescribeDBInstances',
    //   ],
    //   resources: ['*'],
    //   effect: Effect.ALLOW,
    // }));
    const fetchSecretsAndConfigureCrawlerLambda = new Function(this, 'FetchSecretsAndConfigureCrawler', {
      runtime: Runtime.NODEJS_18_X,
      handler: 'index.handler',
      code: Code.fromAsset('lambda'),  // Point to the directory where your Lambda code is
      environment: {
        SECRET_ARN: `arn:aws:secretsmanager:${this.region}:${this.account}:secret:DBSecretD58955BC-Aarz2ser4gmV`,
        GLUE_CRAWLER_NAME: gluCrawlerName,
      },
      role: lambdaExecutionRole, // Specify a role that has permissions to access Secrets Manager and Glue
    });

    // Create an IAM role for Glue Crawler with necessary permissions
    const glueCrawlerRole = new Role(this, 'GlueCrawlerRole', {
      assumedBy: new ServicePrincipal('glue.amazonaws.com'),
    });

    glueCrawlerRole.addToPolicy(new PolicyStatement({
      actions: [
        'glue:*',
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents',
        's3:GetObject',
        's3:PutObject',
        'secretsmanager:GetSecretValue',
        'rds:DescribeDBInstances',
      ],
      resources: ['*'],
      effect: Effect.ALLOW
    }));

    // Create a Glue Database
    const glueDatabase = new CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: 'postgres_glue_db',
        description: 'Glue database for Postgres RDS'
      }
    });

    let host, port, dbname, username, password;
    try {
      host = rdsCredentialsSecret.secretValueFromJson('host').unsafeUnwrap().toString();
      port = rdsCredentialsSecret.secretValueFromJson('port').unsafeUnwrap().toString();
      dbname = rdsCredentialsSecret.secretValueFromJson('dbname').unsafeUnwrap().toString();
      username = rdsCredentialsSecret.secretValueFromJson('username').unsafeUnwrap().toString();
      password = rdsCredentialsSecret.secretValueFromJson('password').unsafeUnwrap().toString();

      console.log(`Host: ${host}`);
      console.log(`Port: ${port}`);
      console.log(`Database: ${dbname}`);
      console.log(`Username: ${username}`);

      // ... (Rest of your configuration using these variables)

    } catch (error) {
      console.error('Error retrieving secrets:', error);
      // ... (Handle the error appropriately)
    }

    // Create a Glue Crawler to crawl the RDS database and populate the Glue Database
    const glueCrawler = new CfnCrawler(this, 'GlueCrawler', {
      role: glueCrawlerRole.roleArn,
      name: gluCrawlerName,
      databaseName: glueDatabase.ref,
      targets: {
        jdbcTargets: [
          {
            connectionName: 'postgres_connection',
            path: 'my_initial_database/public/test_table'
          }
        ]
      },
      configuration: JSON.stringify({
        JDBC_CONNECTION_URL: `jdbc:postgresql://${host}:${port}/${dbname}`,
        PASSWORD: password,
        USERNAME: username,
        STORAGE_DESCRIPTOR: [
          {
            COLUMN_NAME: 'id',
            DATA_TYPE: 'int'
          },
          {
            COLUMN_NAME: 'name',
            DATA_TYPE: 'string'
          },
          {
            COLUMN_NAME: 'age',
            DATA_TYPE: 'int'
          }
        ]
      })
    });

    // Create a named query to show the first 10 records in the test_table
    new CfnNamedQuery(this, 'First10RecordsQuery', {
      database: glueDatabase.ref,
      queryString: 'SELECT * FROM test_table LIMIT 10;',
      name: 'first_10_records_query',
      description: 'Query to retrieve the first 10 records from test_table'
    });

    // Create a named query to show all the tables in the database
    new CfnNamedQuery(this, 'ShowAllTablesQuery', {
      database: glueDatabase.ref,
      queryString: "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';",
      name: 'show_all_tables_query',
      description: 'Query to show all tables in the database'
    });

    // Output the useful information
    new CfnOutput(this, 'GlueDatabaseOutput', {
      value: glueDatabase.ref,
      description: 'Glue Database Name'
    });

    new CfnOutput(this, 'GlueCrawlerOutput', {
      value: glueCrawler.ref,
      description: 'Glue Crawler Name'
    });
  }
}

export default AthenaRDSQueryStack;
