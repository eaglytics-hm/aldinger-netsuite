import { load } from '../google-cloud/bigquery.service';
import { getFile } from '../google-cloud/cloud-storage.service';
import { getLogger } from '../logging.service';

const logger = getLogger(__filename);

export const loadFromGCS = async (filename: string) => {
    const tableName = 'InvoiceLines';
    logger.info(`Loading file ${filename} from GCS to ${tableName}`);

    const loadOptions = {
        dataset: 'AldingerNetSuite',
        table: tableName,
        schema: [
            { name: 'account', type: 'STRING' },
            { name: 'amount', type: 'NUMERIC' },
            { name: 'business_unit', type: 'STRING' },
            { name: 'customer_id', type: 'INT64' },
            { name: 'customer_industry', type: 'STRING' },
            { name: 'customer_legacy_id', type: 'INT64' },
            { name: 'customer_name', type: 'STRING' },
            { name: 'customer_territory', type: 'STRING' },
            { name: 'date', type: 'DATE' },
            { name: 'document_number', type: 'STRING' },
            { name: 'invoice_line', type: 'INT64' },
            { name: 'item_display_name', type: 'STRING' },
            { name: 'item_name', type: 'STRING' },
            { name: 'location_name', type: 'STRING' },
            { name: 'quantity', type: 'NUMERIC' },
            { name: 'shipping_zip', type: 'STRING' },
        ],
    };

    return await load(loadOptions, getFile(filename)).then(() => ({ filename }));
};
