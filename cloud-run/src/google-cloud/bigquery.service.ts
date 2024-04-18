import { BigQuery, TableSchema } from '@google-cloud/bigquery';
import { File } from '@google-cloud/storage';

import { getLogger } from '../logging.service';

const logger = getLogger(__filename);

const client = new BigQuery();
const dataset = client.dataset('NetSuite');

export const load = async (options: { table: string; schema: TableSchema['fields'] }, file: File) => {
    logger.info(`Loading table ${dataset.id}.${options.table}`)

    return await dataset.table(options.table).load(file, {
        schema: { fields: options.schema },
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        createDisposition: 'CREATE_IF_NEEDED',
        writeDisposition: 'WRITE_TRUNCATE',
    });
};
