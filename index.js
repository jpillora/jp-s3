//expose client
exports.Client = require("./client");
//make a global client
const bucket = process.env.BUCKET;
const c = bucket ? new exports.Client({ bucket }) : null;
for (let k of Object.getOwnPropertyNames(exports.Client.prototype)) {
  if (c && c[k] === "function") {
    exports[k] = c[k];
  } else {
    exports[k] = () => {
      throw `must set BUCKET env var for use module global ${k}()`;
    };
  }
}
