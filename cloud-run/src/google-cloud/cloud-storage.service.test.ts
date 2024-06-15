import { getUploadURL } from './cloud-storage.service';

it('getUploadURL', async () => {
    try {
        const result = await getUploadURL();
        expect(result).toBeDefined();
    } catch (error) {
        throw error;
    }
});
