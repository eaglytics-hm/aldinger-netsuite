/**
 *
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */

import { EntryPoints } from 'N/types';
import * as error from 'N/error';
import * as https from 'N/https';
import * as log from 'N/log';
import * as runtime from 'N/runtime';
import * as search from 'N/search';

import dayjs from './dayjs.min';
import { mapValues } from './lodash-amd';

const getConfig = () => {
    const configs = {
        customdeploy_eag_ss_extract_1772: {
            id: 1772,
            parse: (values: any) => ({
                account: values.account?.text,
                amount: parseFloat(values['amount']),
                business_unit: values.class?.text,
                customer_id: values['entityid.customer'],
                customer_industry: values['custentity_esc_industry.customer']?.text,
                customer_legacy_id: values['custentity_ald_legacy_id.customer'],
                customer_name: values['altname.customer'],
                customer_territory: values['territory.customer']?.text,
                date: (() => {
                    const strValue = values.trandate;
                    const date = dayjs(strValue);
                    return date.isValid() ? date.format('YYYY-MM-DD') : null;
                })(),
                document_number: values.tranid,
                invoice_line: parseInt(values.linesequencenumber),
                item_display_name: values['displayname.item'],
                item_name: values['itemid.item'],
                location_name: values.location?.text,
                quantity: parseFloat(values.quantity),
                shipping_zip: values.shipzip,
            }),
            table: 'InvoiceLines',
            schema: [
                { name: 'account', type: 'STRING' },
                { name: 'amount', type: 'NUMERIC' },
                { name: 'business_unit', type: 'STRING' },
                { name: 'customer_id', type: 'STRING' },
                { name: 'customer_industry', type: 'STRING' },
                { name: 'customer_legacy_id', type: 'STRING' },
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
        },
        customdeploy_eag_ss_extract_1722: {
            id: 1722,
            parse: (values: any) => ({
                id: values.entityid,
                name: values.altname,
                stage: values.stage?.text,
                email: values.email,
                phone: values.phone,
                business_unit: values.custentity_ald_businessunit?.text,
                legacy_id: values.custentity_ald_legacy_id,
                terms: values.terms?.text,
                industry: values.custentity_esc_industry?.text,
                territory: values.territory?.text,
                salesrep: values.salesrep?.text,
                leadsource: values.leadsource?.text,
                datecreated: (() => {
                    const strValue = values.datecreated;
                    const date = dayjs(strValue);
                    return date.isValid() ? date.toISOString() : null;
                })(),
                billing_address: values.billaddress,
                billing_state: values.billstate?.text,
                billing_zip: values.billzipcode,
            }),
            table: 'Customers',
            schema: [
                { name: 'id', type: 'STRING' },
                { name: 'name', type: 'STRING' },
                { name: 'stage', type: 'STRING' },
                { name: 'email', type: 'STRING' },
                { name: 'phone', type: 'STRING' },
                { name: 'business_unit', type: 'STRING' },
                { name: 'legacy_id', type: 'STRING' },
                { name: 'terms', type: 'STRING' },
                { name: 'industry', type: 'STRING' },
                { name: 'territory', type: 'STRING' },
                { name: 'salesrep', type: 'STRING' },
                { name: 'leadsource', type: 'STRING' },
                { name: 'datecreated', type: 'TIMESTAMP' },
                { name: 'billing_address', type: 'STRING' },
                { name: 'billing_state', type: 'STRING' },
                { name: 'billing_zip', type: 'STRING' },
            ],
        },
    };

    const { deploymentId } = runtime.getCurrentScript();
    const config = configs[deploymentId as keyof typeof configs];
    if (!config) {
        const _error = error.create({
            name: 'CONFIG_ERROR',
            message: `Deployment id ${deploymentId} not found in configs`,
        });
        log.error(_error.message, _error);
        throw _error;
    }
    return config;
};

const upload = (rows: any[]) => {
    const baseURL = 'https://aldinger-netsuite-backend-jh2uvgk35a-uc.a.run.app';

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

    const { table, schema } = getConfig();
    const loadResponse = https.request({
        method: 'POST',
        url: `${baseURL}/load`,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename, table, schema }),
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
    return search.load({ id: getConfig().id });
};

export const map: EntryPoints.MapReduce.map = (context) => {
    const { values } = JSON.parse(context.value);
    log.audit('values', values);
    const row = mapValues(getConfig().parse(values), (value) => value || null);
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
