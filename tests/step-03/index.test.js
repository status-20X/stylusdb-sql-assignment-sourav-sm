const { parseSelectQuery } = require('../../src/queryParser');

test('Parse SQL Query', () => {
    const query = 'SELECT id, name FROM student';
    const parsed = parseSelectQuery(query);
    expect(parsed).toEqual({
        fields: ['id', 'name'],
        table: 'student',
        whereClauses: [],
        joinCondition: null,
        joinTable: null,
        joinType: null,
        groupByFields: null,
        hasAggregateWithoutGroupBy: false,
        "orderByFields": null,
        "limit": null,
        isDistinct: false
    });
});