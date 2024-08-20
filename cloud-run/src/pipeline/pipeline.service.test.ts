import { loadFromGCS } from './pipeline.service';

it('loadFromGCS', async () => {
    const options = {
        filename: 'ab436172-e8e9-48c0-8138-b781e5c46d7a.ndjson',
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
    };

    try {
        const result = await loadFromGCS(options);
        expect(result).toBeDefined();
    } catch (error) {
        console.error({ error });
        throw error;
    }
});
