const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const sync = require("jp-sync");
const zlib = require("zlib");
const gunzip = sync.promisify(zlib.gunzip);
const gzip = sync.promisify(zlib.gzip);

module.exports = class Client {
  constructor(opts) {
    //bind all
    for (let k of Object.getOwnPropertyNames(Client.prototype)) {
      let v = this[k];
      if (typeof v === "function") {
        this[k] = v.bind(this);
      }
    }
    //required
    this.bucket = opts.bucket;
    if (!this.bucket) {
      throw `s3.Client(opts) requires opts.bucket`;
    }
    //optional
    this.log = opts.log || function() {};
  }

  async readDir(prefix, max = 5) {
    const results = await s3
      .listObjectsV2({
        Bucket,
        MaxKeys: max,
        Prefix: prefix
      })
      .promise();
    return results.Contents;
  }

  async readFile(path) {
    const file = await s3
      .getObject({
        Bucket,
        Key: path
      })
      .promise();
    //auto decompress
    if (file.ContentEncoding === "gzip") {
      file.Body = await gunzip(file.Body);
    }
    return file;
  }

  async readFiles(prefix, max = 5) {
    const stats = await this.readDir(prefix, max);
    this.log(`found #${stats.length} files with prefix '${prefix}'"`);
    const files = await sync.map(5, stats, async stat => {
      const file = await this.readFile(stat.Key);
      file.Key = stat.Key;
      return file;
    });
    return files;
  }

  async writeFile(path, contents) {
    if (!path) {
      throw `path missing`;
    } else if (!contents) {
      throw `contents missing`;
    }
    this.log(`write ${path}`);
    //auto compress
    let encoding = undefined;
    const compressed = await gzip(contents);
    if (compressed.length < contents.length) {
      encoding = "gzip";
      contents = compressed;
    }
    return await s3
      .putObject({
        Bucket,
        Key: path,
        ContentEncoding: encoding,
        Body: contents
      })
      .promise();
  }

  async deleteFile(path) {
    this.log(`delete ${path}`);
    return await s3
      .deleteObject({
        Bucket,
        Key: path
      })
      .promise();
  }

  async deleteFiles(paths) {
    this.log(`delete #${paths.length} files`);
    await sync.each(5, paths, async path => this.deleteFile(path));
    return true;
  }

  async archiveFile(path) {
    if (!path) {
      throw `path missing`;
    }
    if (!/inbox/.test(path)) {
      throw `can only archive files from the inbox (${path})`;
    }
    const newPath = path.replace("inbox", "archive");
    //copy
    this.log(`copy to ${newPath}`);
    await s3
      .copyObject({
        CopySource: `/${Bucket}/${path}`,
        //CopyDest:
        Bucket,
        Key: newPath
      })
      .promise();
    //now safe to delete
    return await this.deleteFile(path);
  }

  async archiveFiles(paths) {
    this.log(`archive #${paths.length} files`);
    await sync.each(5, paths, async path => this.archiveFile(path));
    return true;
  }

  async acquireLock(name, expiry = 3 * 60 * 1000) {
    const key = `${name}.lock`;
    //check if lock already acquired
    let f;
    try {
      f = await this.readFile(key);
    } catch (err) {
      //ignore not found / access denied (also means not found...)
      const msg = err.stack || err.toString();
      if (!/NoSuchKey/.test(msg)) {
        throw `lock read fail: ${msg}`;
      }
    }
    if (f) {
      let data;
      try {
        data = JSON.parse(f.Body.toString());
      } catch (err) {
        await this.deleteFile(key);
        this.log("lock had invalid json");
      }
      if (data) {
        const delta = +new Date() - new Date(data.date);
        //within 3 minutes, lock held
        if (delta < expiry) {
          throw `lock already acquired (${delta}ms ago)`;
        } else {
          this.log("lock expired, overwrite");
        }
      }
    }
    //acquire the lock by writing a new file
    await this.writeJSON(key, {
      date: new Date()
    });
    //written, when done, release to destroy the lock
    const releaseLock = async function() {
      //TODO only delete if your lock
      return await this.deleteFile(key);
    };
    return releaseLock;
  }

  async readJSON(path) {
    let f;
    try {
      f = await this.readFile(path);
    } catch (err) {
      //ignore not found / access denied (also means not found...)
      const msg = err.stack || err.toString();
      if (/NoSuchKey/.test(msg)) {
        return null;
      }
      throw `read-json fail: ${msg}`;
    }
    let o;
    try {
      o = JSON.parse(f.Body.toString());
    } catch (err) {
      throw `read-json parse: ${err}`;
    }
    return o;
  }

  async writeJSON(path, obj) {
    const body = JSON.stringify(obj, null, 2);
    return await this.writeFile(path, body);
  }
};
