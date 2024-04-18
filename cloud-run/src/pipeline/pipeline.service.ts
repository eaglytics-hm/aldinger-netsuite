import { load } from '../google-cloud/bigquery.service';
import { getFile } from '../google-cloud/cloud-storage.service';
import { getLogger } from '../logging.service';

const logger = getLogger(__filename);

export const loadFromGCS = async (filename: string) => {
    const tableName = 'InvoiceLines';
    logger.info(`Loading file ${filename} from GCS to ${tableName}`);

    const loadOptions = {
        table: tableName,
        schema: [
            { name: 'id', type: 'NUMERIC' },
            { name: 'customer', type: 'STRING' },
            { name: 'lead_start_date', type: 'DATE' },
            { name: 'project_status', type: 'STRING' },
            { name: 'revenue', type: 'NUMERIC' },
            { name: 'sales_vendor', type: 'STRING' },
            { name: 'setter', type: 'STRING' },
            { name: 'system_size', type: 'NUMERIC' },
        ],
    };

    return await load(loadOptions, getFile(filename)).then(() => ({filename}));
};
