/**
 *
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */

import { EntryPoints } from 'N/types';
import * as https from 'N/https';
import * as log from 'N/log';
import * as search from 'N/search';

import dayjs from './dayjs.min';
import { mapValues } from './lodash-amd';

const getETLConfig = () => {
    const baseURL = 'https://aldinger-netsuite-backend-master-oisx3fkvoq-uc.a.run.app';

    const response = https.request({ method: 'POST', url: `${baseURL}/upload` });
    const { filename, url: gcsUrl } = <{ filename: string; url: string }>JSON.parse(response.body);

    const upload = (rows: any[]) => {
        const response = https.request({
            method: https.Method.PUT,
            url: gcsUrl,
            headers: { 'Content-Type': 'text/plain' },
            body: rows.map((row) => JSON.stringify(row)).join('\n'),
        });
        if (response.code !== 200) {
            log.error('Upload to GCS', { url: gcsUrl, response });
            return false;
        }

        log.audit('Upload to GCS', { filename });
        return true;
    };

    const loadFromGCS = () => {
        const response = https.request({
            method: 'POST',
            url: `${baseURL}/load`,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename }),
        });

        if (response.code !== 200) {
            log.error('Load from GCS', { filename, response });
            return false;
        }

        log.audit('Load from GCS', { filename, response });
        return true;
    };

    return { upload, loadFromGCS, filename };
};

export const getInputData: EntryPoints.MapReduce.getInputData = () => {
    return search.load({ id: 'customsearch_eag_invoice_lines' });
};

export const map: EntryPoints.MapReduce.map = (context) => {
    const value = JSON.parse(context.value);

    const row = {
        account: <string>value.values['name.account'],
        business_unit: <string>value.values.class?.text,
        customer_id: <number>parseInt(value.values['internalid.customer']?.value),
        customer_industry: <string>value.values['custentity_esc_industry.customer']?.text,
        customer_legacy_id: <number>parseInt(value.values['custentity_ald_legacy_id.customer']),
        customer_name: <string>value.values['altname.customer'],
        customer_territory: <string>value.values['territory.customer']?.text,
        date: (() => {
            const strValue = <string>value.values.trandate;
            const date = dayjs(strValue);
            return date.isValid() ? date.format('YYYY-MM-DD') : null;
        })(),
        document_number: <string>value.values.tranid,
        invoice_line: <number>parseInt(value.values.linesequencenumber),
        item_description: <string>value.values['salesdescription.item'],
        item_display_name: <string>value.values['displayname.item'],
        item_name: <string>value.values['itemid.item'],
        item_rate: <number>value.values.rate,
        location_name: <string>value.values['name.location'],
        quantity: <number>value.values.quantity,
        shipping_zip: <string>value.values.shipzip,
    };
    const data = mapValues(row, (value) => value || null);
    log.debug('data', data);

    context.write('data', data);
};

export const reduce: EntryPoints.MapReduce.reduce = (context) => {
    const { upload, loadFromGCS, filename } = getETLConfig();
    const rows = context.values.map((row) => JSON.parse(row));
    log.debug('rows', rows.slice(2));

    const isUploadSuccess = upload(rows);
    if (!isUploadSuccess) {
        log.error('Upload to GCS failed', { filename });
        return;
    }

    const isLoadFromGCSSuccess = loadFromGCS();
    if (!isLoadFromGCSSuccess) {
        log.error('Load from GCS failed', { filename });
        return;
    }

    log.audit('ETL completed', { filename });
    return;
};
