/**
 *
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
define(["require", "exports", "N/https", "N/log", "N/search"], function (require, exports, https, log, search) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.reduce = exports.getInputData = void 0;
    https = __importStar(https);
    log = __importStar(log);
    search = __importStar(search);
    const lib = () => {
        const baseURL = 'https://us-central1-charge-bee.cloudfunctions.net/solar-works-netsuite-master';
        const getUploadURL = () => {
            const response = https.request({ method: 'POST', url: `${baseURL}/upload` });
            if (response.code !== 200) {
                log.error('lib/get-upload-url', { response });
                return;
            }
            log.audit('lib/get-upload-url', { response });
            return JSON.parse(response.body);
        };
        const upload = (url, rows) => {
            const response = https.request({
                method: https.Method.PUT,
                url,
                headers: { 'Content-Type': 'text/plain' },
                body: rows.map((row) => JSON.stringify(row)).join('\n'),
            });
            if (response.code !== 200) {
                log.error('lib/upload', { url });
                log.error('lib/upload', { response });
                return;
            }
            log.audit('lib/upload', { response });
            return true;
        };
        const loadFromGCS = (filename) => {
            const response = https.request({
                method: 'POST',
                url: `${baseURL}/load`,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filename }),
            });
            if (response.code !== 200) {
                log.error('lib/load-from-gcs', { filename });
                log.error('lib/load-from-gcs', { response });
            }
            log.audit('lib/load-from-gcs', { response });
            return true;
        };
        return { getUploadURL, upload, loadFromGCS };
    };
    const getInputData = () => {
        return search.load({ id: 'customsearch_eag_invoice_lines' });
    };
    exports.getInputData = getInputData;
    // export const map: EntryPoints.MapReduce.map = (context) => {
    //     const value = JSON.parse(context.value);
    //     const row = {
    //         account: <string>value.values.account,
    //         business_unit: <string>value.values.business_unit,
    //         customer_id: <number>value.values.customer_id,
    //         customer_industry: <string>value.values.customer_industry,
    //         customer_legacy_id: <number>value.values.customer_legacy_id,
    //         customer_name: <string>value.values.customer_name,
    //         customer_territory: <string>value.values.customer_territory,
    //         date: <string>value.values.date,
    //         document_number: <string>value.values.document_number,
    //         invoice_line: <number>value.values.invoice_line,
    //         item_description: <string>value.values.item_description,
    //         item_display_name: <string>value.values.item_display_name,
    //         item_name: <string>value.values.item_name,
    //         item_rate: <string>value.values.item_rate,
    //         location: <string>value.values.location,
    //         quantity: <string>value.values.quantity,
    //         shipping_zip: <string>value.values.shipping_zip,
    //     };
    //     context.write('load-to-bigquery', row);
    // };
    const reduce = (context) => {
        // const rows = context.values.map((row) => JSON.parse(row));
        log.debug('rows', context.values.slice(0, 5));
        // log.debug('reduce/rows', rows.slice(0, 5));
        // const { getUploadURL, upload, loadFromGCS } = lib();
        // const uploadValues = getUploadURL();
        // if (!uploadValues) {
        //     log.error('reduce/get-upload-url', {});
        //     return;
        // }
        // const { filename, url } = uploadValues;
        // const isUploadSuccess = upload(url, rows);
        // if (!isUploadSuccess) {
        //     log.error('reduce/upload', {});
        //     return;
        // }
        // const isLoadFromGCSSuccess = loadFromGCS(filename);
        // if (!isLoadFromGCSSuccess) {
        //     log.error('reduce/load-from-gcs', { filename });
        //     return;
        // }
        return;
    };
    exports.reduce = reduce;
});
