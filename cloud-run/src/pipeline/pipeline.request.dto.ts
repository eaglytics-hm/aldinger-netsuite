import Joi from 'joi';
import { ContainerTypes, ValidatedRequestSchema } from 'express-joi-validation';

import { LoadFromGCSOptions as LoadFromGCSBody } from './pipeline.service';

export const LoadFromGCSBodySchema = Joi.object<LoadFromGCSBody>({
    filename: Joi.string().required(),
});

export interface LoadFromGCSBodyRequest extends ValidatedRequestSchema {
    [ContainerTypes.Body]: LoadFromGCSBody;
}
