import express, { NextFunction, Request, Response } from 'express';
import { ValidatedRequest, createValidator } from 'express-joi-validation';
import bodyParser from 'body-parser';
import Joi from 'joi';
import { isObject } from 'lodash';

import { getLogger } from './logging.service';
import { getUploadURL } from './google-cloud/cloud-storage.service';
import { LoadFromGCSBodyRequest, LoadFromGCSBodySchema } from './pipeline/pipeline.request.dto';
import { loadFromGCS } from './pipeline/pipeline.service';

const logger = getLogger(__filename);
const app = express();
const validator = createValidator({ passError: true, joi: { stripUnknown: true } });

app.use(bodyParser.json());

app.use(({ method, path, body }, res, next) => {
    logger.info({ method, path, body });
    res.once('finish', () => {
        logger.info({ method, path, body, status: res.statusCode });
    });
    next();
});

app.use('/upload', (_, res, next) => {
    getUploadURL()
        .then((results) => res.status(200).json(results))
        .catch(next);
});

app.use('/load', validator.body(LoadFromGCSBodySchema), ({ body }: ValidatedRequest<LoadFromGCSBodyRequest>, res, next) => {
    loadFromGCS(body)
        .then((results) => res.status(200).json(results))
        .catch(next);
});

app.use((error: unknown, _: Request, res: Response, next: NextFunction) => {
    if (isObject(error) && 'error' in error && Joi.isError(error.error)) {
        logger.warn({ error: error.error });
        res.status(400).json({ error: error.error });
        return;
    }

    logger.error({ error });
    res.status(500).json({ error });
});

app.listen(8080);
