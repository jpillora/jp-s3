//expose client
exports.Client = require("./client");
//make a global client
if (process.env.BUCKET) {
  const bucket = process.env.BUCKET;
  const c = new exports.Client({ bucket });
  for (let k of Object.getOwnPropertyNames(exports.Client.prototype)) {
    let v = c[k];
    if (typeof v === "function") {
      exports[k] = v;
    }
  }
}
