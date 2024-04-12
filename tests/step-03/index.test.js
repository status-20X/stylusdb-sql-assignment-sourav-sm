const { parseQuery } = require('../../src/queryParser');

test('Parse SQL Query', () => {
    const query = 'SELECT id, name FROM student';
    const parsed = parseQuery(query);
    expect(parsed).toEqual({
        fields: ['id', 'name'],
        table: 'student',
        whereClauses: [],
        joinCondition: null,
        joinTable: null,
        joinType: null,
        groupByFields : null,
        hasAggregateWithoutGroupBy: false,
        orderByFields: null,
        "limit": null,
    });
});