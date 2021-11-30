#!/usr/bin/env node
import * as cdk from '@aws-cdk/core';
import { DunPocInfraStack } from '../lib/dun-poc-infra-stack';

const app = new cdk.App();
new DunPocInfraStack(app, 'DunPocInfraStack');
