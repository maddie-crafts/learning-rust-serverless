#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { InfrastructureStack } from '../lib/earthquake-trends';

const branchName = process.env.BRANCH_NAME || 'main';

const account = process.env.CDK_DEFAULT_ACCOUNT || 'ACCOUNT_ID';
const region = process.env.CDK_DEFAULT_REGION || 'REGION';

const datadogApiKey = process.env.DATADOG_API_KEY;

if (!datadogApiKey) {
  throw new Error('Missing required environment variable: DATADOG_API_KEY');
}

const app = new cdk.App();

new InfrastructureStack(app, `earthquake-trends-${branchName}`, {
  branchName,
  datadogApiKey,
  env: {
    account,
    region,
  },
});