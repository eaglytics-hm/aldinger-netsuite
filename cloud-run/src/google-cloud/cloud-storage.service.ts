import { Storage } from '@google-cloud/storage';
import { v4 as uuid4 } from 'uuid';

const client = new Storage();

const bucket = '460837084029-us-netsuite';

export const getFile = (filename: string) => {
    return client.bucket(bucket).file(filename);
};

export const getUploadURL = async () => {
    const filename = `${uuid4()}.ndjson`;

    const [url] = await getFile(filename).getSignedUrl({
        version: 'v4',
        action: 'write',
        expires: Date.now() + 60 * 60 * 1000,
        contentType: 'text/plain',
    });

    return { filename, url };
};
