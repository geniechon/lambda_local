'use strict';
console.log('Loading function');


exports.handler = (event, context, callback) => {
    /* Process the list of records and transform them */

    // const flatten = require('flat').flatten
    for (const record of event.records) {
        const payload = flattenData(parseKinesisData(JSON.stringify(record.data)))
        console.log(payload)
    }
}

const flattenData = (data) => {
    return flatten(data)
}

const parseKinesisData = (data) => {
    return JSON.parse(Buffer.from(data, 'base64').toString('utf-8'));
}

function traverseAndFlatten(currentNode, target, flattenedKey) {
    for (var key in currentNode) {
        if (currentNode.hasOwnProperty(key)) {
            var newKey;
            if (flattenedKey === undefined) {
                newKey = key;
            } else {
                newKey = flattenedKey + '.' + key;
            }

            var value = currentNode[key];
            if (typeof value === "object") {
                traverseAndFlatten(value, target, newKey);
            } else {
                target[newKey] = value;
            }
        }
    }
}

function flatten(obj) {
    var flattenedObject = {};
    traverseAndFlatten(obj, flattenedObject);
    return flattenedObject;
}