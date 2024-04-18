import Joi from 'joi';
import { ContainerTypes, ValidatedRequestSchema } from 'express-joi-validation';

type LoadFromGCSBody = { filename: string };

export const LoadFromGCSBodySchema = Joi.object<LoadFromGCSBody>({
    filename: Joi.string().required(),
});

export interface LoadFromGCSBodyRequest extends ValidatedRequestSchema {
    [ContainerTypes.Body]: LoadFromGCSBody;
}
