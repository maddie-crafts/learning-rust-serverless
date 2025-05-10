import { RustFunction } from '@cdklabs/aws-lambda-rust';
import { Duration } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import { LayerVersion, Architecture } from 'aws-cdk-lib/aws-lambda';

interface EarthquakeTrendsLambdaProps {
  functionName: string;
  datadogApiKey: string;
}

export class EarthquakeTrendsLambda extends Construct {
  public readonly lambdaFunction: RustFunction;

  constructor(scope: Construct, id: string, props: EarthquakeTrendsLambdaProps) {
    super(scope, id);

    const datadogLayer = LayerVersion.fromLayerVersionArn(this, "DatadogLayer", 
      "arn:aws:lambda:eu-west-1:464622532012:layer:Datadog-Extension-ARM:55"
    );

    this.lambdaFunction = new RustFunction(this, 'EarthquakeTrendsFunction', {
      entry: '../rust_lambda/Cargo.toml',
      binaryName: 'bootstrap',
      functionName: props.functionName,
      timeout: Duration.seconds(10),
      architecture: Architecture.ARM_64,
      environment: {
        LOG_LEVEL: "info",
        APP_ENVIRONMENT: "production",
        DD_SITE: "datadoghq.eu",
        DD_API_KEY: props.datadogApiKey,
        DD_ENV: "production",
        DD_SERVICE: "earthquake-trends-api",
        AWS_LAMBDA_EXEC_WRAPPER: "/opt/datadog_wrapper",
      },
      layers: [datadogLayer],
    });

    new apigateway.LambdaRestApi(this, 'EarthquakeTrendsApi', {
      handler: this.lambdaFunction,
      restApiName: 'Earthquake Trends API',
      proxy: true,
    });
  }
}