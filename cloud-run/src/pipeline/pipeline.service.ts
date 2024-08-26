import { BigQuery, TableSchema } from '@google-cloud/bigquery';

import { getFile } from '../google-cloud/cloud-storage.service';
import { getLogger } from '../logging.service';

const logger = getLogger(__filename);
const client = new BigQuery();
const dataset = client.dataset('RawData_INV');

export type LoadFromGCSOptions = { filename: string; table: string; schema: TableSchema['fields'] };

export const loadFromGCS = async ({ filename, table, schema }: LoadFromGCSOptions) => {
    logger.info(`Loading file ${filename} from GCS to ${table}`);

    const file = getFile(filename);
    await dataset.table(table).load(file, {
        schema: { fields: schema },
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        createDisposition: 'CREATE_IF_NEEDED',
        writeDisposition: 'WRITE_TRUNCATE',
    });
    return { filename };
};
