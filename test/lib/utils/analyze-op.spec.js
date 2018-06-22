'use strict';

const Promise = require('bluebird');
const times = require('lodash/times');
const map = require('lodash/map');
const mean = require('lodash/mean');
const { analyzeOp, getMemoryUsage } = require('../../../lib/utils');

const MB = 1024 * 1024;
const MEM_DIFF = MB * 100;
const TIME_DIFF = 100;

describe('Operation Analytics', () => {
    it('should throw an error if constructed without a fn', () => {
        expect(() => analyzeOp()).toThrowError('Operation analytics requires a valid op function');
    });

    it('should throw an error if constructed without a fn', () => {
        expect(() => analyzeOp(jest.fn())).toThrowError('Operation analytics requires a valid index');
    });

    it('should return a function', () => {
        const op = jest.fn();
        const analyzedFn = analyzeOp(op, 0);
        expect(analyzedFn).toBeFunction();
        expect(op).not.toHaveBeenCalled();
    });

    describe('when using an array', () => {
        it('should mutate the analytics object being passed', async () => {
            const count = 10;
            const input = times(count);
            const op = (data) => {
                const add = n => Promise.delay(count).then(() => n + 1);
                return Promise.mapSeries(data, add);
            };
            const analyticsObj = { time: [], size: [], memory: [] };
            const numberOfOps = 5;
            const expectedMem = getMemoryUsage();

            await Promise.each(times(numberOfOps), async (index) => {
                const analyzedFn = analyzeOp(op, index);

                const result = await analyzedFn(analyticsObj, input);

                expect(result).toEqual(map(input, n => n + 1));

                expect(analyticsObj).toContainAllKeys(['time', 'size', 'memory']);

                const expectedTime = count * count;
                expect(analyticsObj.time[index]).toBeWithin(expectedTime, expectedTime + TIME_DIFF);

                expect(analyticsObj.size[index]).toEqual(count);

                const memLower = expectedMem - MEM_DIFF;
                const memUpper = expectedMem + MEM_DIFF;
                expect(analyticsObj.memory[index]).toBeWithin(memLower, memUpper);
            });

            expect(analyticsObj.time).toBeArrayOfSize(numberOfOps);
            expect(analyticsObj.size).toBeArrayOfSize(numberOfOps);
            expect(analyticsObj.memory).toBeArrayOfSize(numberOfOps);
        });
    });

    describe('when using hit.hits', () => {
        it('should mutate the analytics object being passed', async () => {
            const count = 10;
            const input = {
                hits: {
                    hits: times(count)
                }
            };
            const op = (data) => {
                const add = n => Promise.delay(count).then(() => n * 2);
                return Promise.mapSeries(data.hits.hits, add)
                    .then(hits => ({ hits: { hits } }));
            };
            const analyticsObj = { time: [], size: [], memory: [] };
            const numberOfOps = 5;
            const expectedMem = getMemoryUsage();

            await Promise.each(times(numberOfOps), async (index) => {
                const analyzedFn = analyzeOp(op, index);

                const result = await analyzedFn(analyticsObj, input);
                const expectedResult = {
                    hits: {
                        hits: times(count, n => n * 2)
                    }
                };
                expect(result).toEqual(expectedResult);

                expect(analyticsObj).toContainAllKeys(['time', 'size', 'memory']);

                const expectedTime = count * count;
                expect(analyticsObj.time[index]).toBeWithin(expectedTime, expectedTime + TIME_DIFF);

                expect(analyticsObj.size[index]).toEqual(count);

                const memLower = expectedMem - MEM_DIFF;
                const memUpper = expectedMem + MEM_DIFF;
                expect(analyticsObj.memory[index]).toBeWithin(memLower, memUpper);
            });

            expect(analyticsObj.time).toBeArrayOfSize(numberOfOps);
            expect(analyticsObj.size).toBeArrayOfSize(numberOfOps);
            expect(analyticsObj.memory).toBeArrayOfSize(numberOfOps);
        });
    });

    it('should return size of 0 if returning a non-array as the result', async () => {
        const analyticsObj = { time: [], size: [], memory: [] };
        const op = () => 'hello';
        const analyzedFn = analyzeOp(op, 0);
        await analyzedFn(analyticsObj, []);
        expect(analyticsObj.size).toBeArrayOfSize(1);
        expect(analyticsObj.size[0]).toEqual(0);
    });

    xit('should be performant', async () => {
        const runTest = (size) => {
            const analyticsObj = { time: [], size: [], memory: [] };
            let str = '';
            return Promise.each(times(100), (index) => {
                const op = () => {
                    str += JSON.stringify(times(size));
                    return Promise.delay(1);
                };
                const analyzedFn = analyzeOp(op, index);
                return analyzedFn(analyticsObj, []);
            }).then(() => {
                expect(str.length).toBeGreaterThan(size);
                const meanMem = mean(analyticsObj.memory);
                const meanTime = mean(analyticsObj.time);
                return { meanTime, meanMem };
            });
        };

        await Promise.delay(1000); // give time to garbage collect

        const smallTest = await runTest(1000);

        await Promise.delay(1000); // give time to garbage collect

        const bigTest = await runTest(100000);
        console.dir({ bigTest, smallTest }); // eslint-disable-line
    });
});

