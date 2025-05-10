import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { EarthquakeTrendsLambda } from './lambda-function';

export interface InfrastructureStackProps extends cdk.StackProps {
    branchName: string;
    datadogApiKey: string;
}
  
export class InfrastructureStack extends cdk.Stack {
constructor(scope: Construct, id: string, props?: InfrastructureStackProps) {
    super(scope, id, props);
    const branchName = props?.branchName;
    const projectName = `earthquake-trends-${branchName}`;

    const earthquakeTrendslambdaFunction = new EarthquakeTrendsLambda(this, 'EarthquakeTrendsLambda', {
        functionName: `${projectName}-lambda`,
        datadogApiKey: props?.datadogApiKey || '',
    });

    }
}