import { BigQuery, TableSchema } from '@google-cloud/bigquery';
import { File } from '@google-cloud/storage';

import { getLogger } from '../logging.service';

const logger = getLogger(__filename);

const client = new BigQuery();

type LoadOptions = {
    dataset: string;
    table: string;
    schema: TableSchema['fields'];
};

export const load = async ({ dataset, table, schema }: LoadOptions, file: File) => {
    logger.info(`Loading table ${dataset}.${table}`);

    return await client
        .dataset(dataset)
        .table(table)
        .load(file, {
            schema: { fields: schema },
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            createDisposition: 'CREATE_IF_NEEDED',
            writeDisposition: 'WRITE_TRUNCATE',
        });
};
