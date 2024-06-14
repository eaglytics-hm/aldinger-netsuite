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

const getETLConfig = () => {
    const baseURL = 'https://aldinger-netsuite-backend-master-jh2uvgk35a-uc.a.run.app';

    const response = https.request({ method: 'POST', url: `${baseURL}/upload` });
    if (response.code !== 200) {
        log.error('Create upload file', { response: response.body });
        return false;
    }
    const { filename, url: gcsUrl } = <{ filename: string; url: string }>JSON.parse(response.body);

    const upload = (rows: any[]) => {
        const response = https.request({
            method: https.Method.PUT,
            url: gcsUrl,
            headers: { 'Content-Type': 'text/plain' },
            body: rows.map((row) => JSON.stringify(row)).join('\n'),
        });
        if (response.code !== 200) {
            log.error('Upload to GCS', { url: gcsUrl, response: response.body });
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
            log.error('Load from GCS', { filename, response: response.body });
            return false;
        }

        log.audit('Load from GCS', { filename, response: response.body });
        return true;
    };

    return { upload, loadFromGCS, filename };
};

export const getInputData: EntryPoints.MapReduce.getInputData = () => {
    return search.load({ id: 'customsearch_ald_invoice_line_detail' });
};

export const map: EntryPoints.MapReduce.map = (context) => {
    const result = <search.Result>JSON.parse(context.value).values;
    log.debug('result', result);

    const row = mapValues(
        {
            account: <string>result.getText('account'),
            amount: <number>Number(result.getValue('amount')),
            business_unit: <string>result.getText('class'),
            customer_id: <string>result.getValue('entityid.customer'),
            customer_industry: <string>result.getText('custentity_esc_industry.customer'),
            customer_legacy_id: <string>result.getValue('custentity_ald_legacy_id.customer'),
            customer_name: <string>result.getValue('altname.customer'),
            customer_territory: <string>result.getText('territory.customer'),
            date: (() => {
                const date = dayjs(<string | Date>result.getValue('trandate'));
                return date.isValid() ? date.format('YYYY-MM-DD') : null;
            })(),
            document_number: <string>result.getValue('tranid'),
            invoice_line: <number>Number(result.getValue('linesequencenumber')),
            item_display_name: <string>result.getValue('displayname.item'),
            item_name: <string>result.getValue('itemid.item'),
            location_name: <string>result.getText('location'),
            quantity: <number>Number(result.getValue('quantity')),
            shipping_zip: <string>result.getValue('shipzip'),
        },
        (value) => value || null,
    );

    context.write('data', row);
};

export const reduce: EntryPoints.MapReduce.reduce = (context) => {
    const etlConfig = getETLConfig();
    if (!etlConfig) {
        const _error = error.create({ name: 'API_ERROR', message: 'Create ETL config failed' });
        log.error(_error.message, _error);
        throw _error;
    }

    const rows = context.values.map((row) => JSON.parse(row));
    const { upload, loadFromGCS, filename } = etlConfig;

    const isUploadSuccess = upload(rows);
    if (!isUploadSuccess) {
        const _error = error.create({ name: 'API_ERROR', message: 'Upload to GCS failed' });
        log.error(_error.message, _error);
        throw _error;
    }

    const isLoadFromGCSSuccess = loadFromGCS();
    if (!isLoadFromGCSSuccess) {
        const _error = error.create({ name: 'API_ERROR', message: 'Load from GCS failed' });
        log.error(_error.message, _error);
        throw _error;
    }

    log.audit('ETL completed', { filename });
};
