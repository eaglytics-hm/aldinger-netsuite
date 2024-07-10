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

const configs = {
    customdeploy_eag_ss_extract_1772: {
        id: 1772,
        parse: (values: any) => ({
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
        }),
        table: 'InvoiceLine',
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
            id: <string>values.entityid,
            name: <string>values.altname,
            stage: <string>values.stage?.text,
            email: <string>values.email,
            phone: <string>values.custentity_ald_businessunit?.text,
            legacy_id: values.custentity_ald_legacy_id,
            terms: <string>values.terms?.text,
            industry: <string>values.custentity_esc_industry?.text,
            territory: <string>values.territory?.text,
            salesrep: <string>values.salesrep?.text,
            leadsource: <string>values.leadsource?.text,
            datecreated: (() => {
                const strValue = <string>values.datecreated;
                const date = dayjs(strValue);
                return date.isValid() ? date.toISOString() : null;
            })(),
            billing_address: <string>values.billaddress,
            billing_state: <string>values.billstate?.text,
            billing_zip: <string>values.billzipcode,
        }),
        table: 'Customer',
        schema: [
            { name: 'id', type: 'STRING' },
            { name: 'name', type: 'STRING' },
            { name: 'stage', type: 'STRING' },
            { name: 'email', type: 'STRING' },
            { name: 'phone', type: 'STRING' },
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

export const getInputData: EntryPoints.MapReduce.getInputData = () => {
    const { deploymentId } = runtime.getCurrentScript();
    return search.load({ id: configs[deploymentId as keyof typeof configs].id });
};

export const map: EntryPoints.MapReduce.map = (context) => {
    const { deploymentId } = runtime.getCurrentScript();
    const { values } = JSON.parse(context.value);
    log.audit('values', values);
    const row = mapValues(configs[deploymentId as keyof typeof configs].parse(values), (value) => value || null);
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
