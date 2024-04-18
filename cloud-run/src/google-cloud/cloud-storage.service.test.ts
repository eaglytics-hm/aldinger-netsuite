import { getUploadURL } from './cloud-storage.service';

it('get-upload-url', async () => {
    return await getUploadURL()
        .then((result) => {
            console.log({ result });
            expect(result).toBeDefined();
        })
        .catch((error) => {
            console.error({ error });
            throw error;
        });
});
