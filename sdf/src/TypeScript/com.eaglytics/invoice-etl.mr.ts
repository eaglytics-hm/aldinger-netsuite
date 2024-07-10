/**
 *
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */

import { EntryPoints } from 'N/types';
import * as error from 'N/error';
import * as https from 'N/https';
import * as log from 'N/log';
import * as search from 'N/search';

import dayjs from './dayjs.min';
import { mapValues } from './lodash-amd';

const upload = (rows: any[]) => {
    const baseURL = 'https://aldinger-netsuite-backend-master-jh2uvgk35a-uc.a.run.app';

    const getUploadUrlResponse = https.request({ method: 'POST', url: `${baseURL}/upload` });
    if (getUploadUrlResponse.code !== 200) {
        log.error('Create upload file', { response: getUploadUrlResponse.body });
        const _error = error.create({ name: 'API_ERROR', message: 'Create ETL config failed' });
        return [_error, null] as const;
    }

    const { filename, url: gcsUrl } = <{ filename: string; url: string }>JSON.parse(getUploadUrlResponse.body);

    const uploadResponse = https.request({
        method: https.Method.PUT,
        url: gcsUrl,
        headers: { 'Content-Type': 'text/plain' },
        body: rows.map((row) => JSON.stringify(row)).join('\n'),
    });
    if (uploadResponse.code !== 200) {
        log.error('Upload to GCS', { url: gcsUrl, response: uploadResponse.body });
        const _error = error.create({ name: 'API_ERROR', message: 'Upload to GCS failed' });
        log.error(_error.message, _error);
        return [_error, null] as const;
    }

    const loadResponse = https.request({
        method: 'POST',
        url: `${baseURL}/load`,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename }),
    });

    if (loadResponse.code !== 200) {
        log.error('Load from GCS', { filename, response: loadResponse.body });
        const _error = error.create({ name: 'API_ERROR', message: 'Load from GCS failed' });
        log.error(_error.message, _error);
        return [_error, null] as const;
    }

    log.audit('Load from GCS', { filename, response: loadResponse.body });
    return [null, filename] as const;
};

export const getInputData: EntryPoints.MapReduce.getInputData = () => {
    return search.load({ id: 'customsearch_ald_invoice_line_detail' });
};

export const map: EntryPoints.MapReduce.map = (context) => {
    const { values } = JSON.parse(context.value);

    const row = mapValues(
        {
            account: <string>values.account?.text,
            amount: <number>parseFloat(values['amount']),
            business_unit: <string>values.class?.text,
            customer_id: <string>values['entityid.customer'],
            customer_industry: <string>values['custentity_esc_industry.customer']?.text,
            customer_legacy_id: <string>values['custentity_ald_legacy_id.customer'],
            customer_name: <string>values['altname.customer'],
            customer_territory: <string>values['territory.customer']?.text,
            date: (() => {
                const strValue = <string>values.trandate;
                const date = dayjs(strValue);
                return date.isValid() ? date.format('YYYY-MM-DD') : null;
            })(),
            document_number: <string>values.tranid,
            invoice_line: <number>parseInt(values.linesequencenumber),
            item_display_name: <string>values['displayname.item'],
            item_name: <string>values['itemid.item'],
            location_name: <string>values.location?.text,
            quantity: <number>parseFloat(values.quantity),
            shipping_zip: <string>values.shipzip,
        },
        (value) => value || null,
    );

    context.write('data', row);
};

export const reduce: EntryPoints.MapReduce.reduce = (context) => {
    const rows = context.values.map((row) => JSON.parse(row));

    const [_error, filename] = upload(rows);
    if (_error && !filename) {
        log.error(_error.message, _error);
        throw _error;
    }
    log.audit('ETL completed', { filename });
};
