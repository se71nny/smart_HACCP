import express from "express";
import cors from "cors";
import path from "path";
import morgan from "morgan";
import dotenv from "dotenv";
import moment from "moment";
import fs from "fs";
import cluster, { Worker } from "cluster";
import { client } from "jsmodbus";
import { Socket, SocketConnectOpts } from "net";
import Schedule from "./schedule";

dotenv.config();

import routes from "./routes/index";
import config from "../config/Config";
import { getPool } from "../config/DB";
import mqtt from "mqtt";
import dashboardWebsocket from "./websocket/dash";
import processWebsocket from "./websocket/process";
import newDashboardWebsocket from "./websocket/newdash";

const WebSocketS = require("ws").Server;
const socket = new Socket();
var PLC_options: any = new Object();

var mclient: any = [];

const app = express();

var Total_PLC_Data: any;
var Total_Data1 = new Array();

app.use(cors());
app.use(morgan("dev"));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, "public")));

app.use("/api/item", routes.item);
app.use("/api/login", routes.login);
app.use("/api/order", routes.order);
app.use("/api/work", routes.work);
app.use("/api/robot", routes.robot);
app.use("/api/kpi", routes.kpi);
app.use("/api/user", routes.user);
app.use("/api/company", routes.company);
app.use("/api/ccp", routes.ccp);
app.use("/api/dash", routes.dash);
app.use("/api/process", routes.process);
app.use("/api/process1", routes.process1);
app.use("/api/newdash", routes.newdash);

const connectionDB = async () => {
  const pool = await getPool();
  var conn = await pool.getConnection();
  return conn;
};

async function Read() {
  Schedule();
  var Total_Data = new Array();
  var conn = await connectionDB();
  await conn.beginTransaction();

  try {
    // // 파티션 확인하기
    // let partitionTime = moment().add(1, "d").format('YYYY-MM-DD')

    // var query = "SELECT P.TABLE_NAME, P.PARTITION_NAME, P.PARTITION_ORDINAL_POSITION, P.PARTITION_DESCRIPTION, P.TABLE_ROWS "
    //     + `FROM tb_log_retention LR INNER JOIN information_schema.PARTITIONS P ON P.TABLE_SCHEMA = '${config.development.DB.database}' AND P.TABLE_NAME = LR.log_table_name `
    //     + `WHERE P.PARTITION_DESCRIPTION ='\''${partitionTime}\''' and P.TABLE_NAME ='tb_sensor_data';`

    // var [rows]: any = await conn.query(query)

    // if (rows.length == 0) {
    //     var query = "CALL test_data_partition(2, @r);"

    //     await conn.query(query)
    // }

    var query = "select distinct m.plc_idx, m.mt_mode from tb_matching_list m order by plc_idx; ";
    var [result_PLC_Data]: any = await conn.query(query);

    query =
      " select distinct plc.plc_idx ,plc.plc_ip, plc.plc_port, plc.plc_code, pr.pr_ip, pr.pr_port, pr.pr_path from tb_matching_list m " +
      "left outer join tb_plc_info plc on plc.plc_idx = m.plc_idx and m.mt_mode = 'plc' or m.mt_mode = 'mqtt'" +
      "left outer join tb_parsing_info pr on pr.pr_idx = m.plc_idx and m.mt_mode = 'parsing' order by plc.plc_idx ;";

    var [result_IP_Port]: any = await conn.query(query);

    for (var i = 0; i < result_PLC_Data.length; i++) {
      query =
        "select mt.mt_idx, mt.fa_idx, mt.sn_idx, mt.mt_mode, ad.ad_function_code, ad.ad_address, ad.ad_formula, ad.ad_type, ad.ad_complement, fa.fa_name, fa.fa_group from tb_matching_list as mt " +
        "left outer join tb_address_info as ad on mt.plc_ad_idx = ad.ad_idx " +
        "left outer join tb_facility_info as fa on mt.fa_idx = fa.fa_idx " +
        "where ad.plc_idx = " +
        result_PLC_Data[i].plc_idx +
        " and mt.mt_mode = '" +
        result_PLC_Data[i].mt_mode +
        "' ;";

      var [result_PLC_Addresses]: any = await conn.query(query); // 매칭 list

      for (var j = 0; j < result_PLC_Addresses.length; j++) {
        var idx = result_PLC_Addresses[j].sn_idx;

        query = `select sn_idx, sn_name, sn_abbreviation, sn_unit from tb_sensor_info where sn_idx in (${idx});`;

        var [result_Sensors]: any = await conn.query(query); // 센서 list

        var name = new Array();
        var abbreviation = new Array();

        for (var k = 0; k < result_Sensors.length; k++) {
          name.push(result_Sensors[k].sn_name);
          abbreviation.push(result_Sensors[k].sn_abbreviation); // info
        }

        result_PLC_Addresses[j].sn_name = name.join("/");
        result_PLC_Addresses[j].sn_abbreviation = abbreviation.join("/");
      }

      if (result_IP_Port[i].pr_path == null || result_IP_Port[i].pr_path == undefined) {
        //PLC
        PLC_options = {
          ...PLC_options,
          [i]: {
            IP: result_IP_Port[i].plc_ip,
            port: result_IP_Port[i].plc_port,
            Code: result_IP_Port[i].plc_code,
            Path: result_IP_Port[i].pr_path,
            addresses: result_PLC_Addresses,
          },
        };
      } else {
        //Parsing
        PLC_options = {
          ...PLC_options,
          [i]: {
            IP: result_IP_Port[i].pr_ip,
            port: result_IP_Port[i].pr_port,
            Code: 0,
            Path: `${result_IP_Port[i].pr_path}`,
            addresses: result_PLC_Addresses,
          },
        };
      }
    }

    await conn.commit();
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }

  var input_num = 0;

  var tempArray = new Array();
  if (result_IP_Port.length > 0) {
    for (var i = 0; i < result_IP_Port.length; i++) {
      // 등록된 panel list
      // if (!tempArray.includes(result_IP_Port[i].plc_ip)) {
      tempArray.push(result_IP_Port[i].plc_ip);
      input_num++;
      // }
    }
  } else {
    input_num = 1;
  }

  for (var i = 0; i < input_num; i++) {
    var worker = await cluster.fork({ fork: i });

    worker.on("message", async function (message: any) {
      if (!JSON.stringify(message).includes("writeData")) {
        Total_Data.push(message);
        if (Total_Data.length == result_IP_Port.length) {
          Total_Data1 = Total_Data;
          Total_Data = [];
        }
      }
    });
  }

  const workers: any = cluster.workers;

  Object.keys(workers).forEach((key) => {
    const worker = workers[key];

    const sendData = PLC_options[String(Number(key) - 1)];

    if (sendData !== undefined) {
      worker.send(sendData);
    }
  });

  const webSocketServer = new WebSocketS({ port: 8001, host: '127.0.0.1' });

  webSocketServer.on("connection", (ws: any, request: any) => {
    // 1) 연결 클라이언트 IP 취득
    const ip = request.headers["x-forwarded-for"] || request.connection.remoteAddress;
    console.log(`새로운 클라이언트[${ip}] 접속`); // 2) 클라이언트에게 메시지 전송

    // ws.on('message', async (data: any) => {
    //     dashboardWebsocket(data, ws);
    // })

    ws.on("message", async (data: any) => {
      processWebsocket(data, ws);
      newDashboardWebsocket(data, ws);
    });

    ws.on("error", (error: any) => {
      console.log(`클라이언트[${ip}] 연결 에러발생 : ${error}`);
    });
    ws.on("close", () => {
      console.log(`클라이언트[${ip}] 웹소켓 연결 종료`);
    });
  });
}
if (cluster.isMaster) {
  console.log(`마스터 프로세스 아이디 :  ${process.pid}`);

  Read();
  // Schedule()

  cluster.on("fork", (worker: Worker) => {
    console.log(`워커 ${worker.id} (${worker.process.pid}) fork`);
  });

  cluster.on("exit", (worker: Worker) => {
    var workers: any = cluster.workers;

    console.log(`${worker.process.pid}번 워커가 종료되었습니다.`);

    if (Object.keys(workers).length == 0) {
      Read();
    }
  });

  app.listen(4031, async () => {
    console.log(`Server listening on port: 4031`);
  });
} else if (cluster.isWorker) {
  var PLC_addresses_01 = new Array();
  var PLC_addresses_02 = new Array();
  var PLC_addresses_03 = new Array();
  var PLC_addresses_04 = new Array();
  var PLC_addresses_77 = new Array();

  let dataObj = new Object();
  let dataArr = new Array();

  var refresh: any;
  let checkData: any;
  let timer = 1000;

  let writeObj: any = new Object();

  function Read_PLC_Data(functionCode: string, Total_PLC_Data: any) {
    return new Promise(function (resolve, reject) {
      if (mclient.connectionState === "online") {
        if (functionCode == "01") {
          if (PLC_addresses_01.length > 0) {
            for (var i of PLC_addresses_01) {
              mclient
                .readCoils(i * 100, 100)
                .then(function (resp: any) {
                  // 01

                  Total_PLC_Data[String("01-" + resp.request.body.start)] = resp.response.body.valuesAsArray;
                  if (resp.request.body.start == PLC_addresses_01[PLC_addresses_01.length - 1] * 100) {
                    resolve(Total_PLC_Data);
                  }
                })
                .catch((error: any) => {
                  console.log("TimeOut IP: " + Total_PLC_Data.IP);
                  checkData = "read_end";
                  socket.destroy();
                  socket.end();
                  console.log(error);
                  clearInterval(refresh);
                });
            }
          } else {
            resolve(Total_PLC_Data);
          }
        } else if (functionCode == "02") {
          if (PLC_addresses_02.length > 0) {
            for (var i of PLC_addresses_02) {
              mclient
                .readDiscreteInputs(i * 100, 100)
                .then(function (resp: any) {
                  // 02

                  Total_PLC_Data[String("02-" + resp.request.body.start)] = resp.response.body.valuesAsArray;
                  if (resp.request.body.start == PLC_addresses_02[PLC_addresses_02.length - 1] * 100) {
                    resolve(Total_PLC_Data);
                  }
                })
                .catch((error: any) => {
                  console.log("TimeOut IP: " + Total_PLC_Data.IP);
                  checkData = "read_end";
                  socket.destroy();
                  socket.end();
                  console.log(error);
                  clearInterval(refresh);
                });
            }
          } else {
            resolve(Total_PLC_Data);
          }
        } else if (functionCode == "03") {
          if (PLC_addresses_03.length > 0) {
            for (var i of PLC_addresses_03) {
              mclient
                .readHoldingRegisters(i * 100, 100)
                .then(function (resp: any) {
                  // 03
                  Total_PLC_Data[String("03-" + resp.request.body.start)] = resp.response.body.valuesAsArray;
                  // console.log(resp.response)

                  if (resp.request.body.start == PLC_addresses_03[PLC_addresses_03.length - 1] * 100) {
                    resolve(Total_PLC_Data);
                  }
                })
                .catch((error: any) => {
                  console.log("TimeOut IP: " + Total_PLC_Data.IP);
                  checkData = "read_end";
                  socket.destroy();
                  socket.end();
                  console.log(error);
                  clearInterval(refresh);
                });
            }
          } else {
            resolve(Total_PLC_Data);
          }
        } else if (functionCode == "04") {
          if (PLC_addresses_04.length > 0) {
            for (var i of PLC_addresses_04) {
              mclient
                .readInputRegisters(i * 100, 100)
                .then(function (resp: any) {
                  // 04
                  Total_PLC_Data[String("04-" + resp.request.body.start)] = resp.response.body.valuesAsArray;
                  // console.log(resp.response.body.valuesAsArray)

                  if (resp.request.body.start == PLC_addresses_04[PLC_addresses_04.length - 1] * 100) {
                    resolve(Total_PLC_Data);
                  }
                })
                .catch((error: any) => {
                  console.log("TimeOut IP: " + Total_PLC_Data.IP);
                  checkData = "read_end";
                  socket.destroy();
                  socket.end();
                  console.log(error);
                  clearInterval(refresh);
                });
            }
          } else {
            resolve(Total_PLC_Data);
          }
        } else if (functionCode == "77") {
          if (PLC_addresses_77.length > 0) {
            if (Total_PLC_Data.path != undefined) {
              for (var i of PLC_addresses_77) {
                var file_name = Total_PLC_Data.path;
                var lastData: any = new Array();

                fs.readFile(file_name, function (err, data) {
                  if (err) console.log(err);
                  var allData = data.toString().split("\n");

                  for (var j = 1; j <= allData.length; j++) {
                    if (allData[allData.length - j].replace(/(\r\n|\n|\r)/gm, "") !== "") {
                      lastData = allData[allData.length - j];
                      lastData = lastData.split(",");

                      Total_PLC_Data[String("77-" + i)] = lastData;

                      resolve(Total_PLC_Data);
                      break;
                    }
                  }
                });
              }
            }
          } else {
            resolve(Total_PLC_Data);
          }
        }
      }
    });
  }

  var mqtt_count = 0;
  var save_count = 0;

  socket.on("connect", function (message: any) {
    console.log("Open IP: " + Total_PLC_Data.IP);

    clearInterval(refresh);
    refresh = setInterval(() => {
      if (Total_PLC_Data.Mode[0] !== "mqtt") {
        if (mclient.connectionState == "online") {
          if (Object.keys(writeObj).length > 0) {
            if (
              Total_PLC_Data.IP == writeObj.ip &&
              Total_PLC_Data.port == writeObj.port &&
              Total_PLC_Data.code == writeObj.code
            ) {
              // console.log('hello')
              mclient.writeMultipleRegisters(Number(writeObj.address), writeObj.value).then((res: any) => {
                (<any>process).send("writeData");
                writeObj = {};
              });
            }
          }

          save_count++;
          Read_PLC_Data("01", Total_PLC_Data)
            .then((data: any) => {
              return Read_PLC_Data("02", data);
            })
            .then((data: any) => {
              return Read_PLC_Data("03", data);
            })
            .then((data: any) => {
              return Read_PLC_Data("04", data);
            })
            .then((data: any) => {
              return Read_PLC_Data("77", data);
            })
            .then(async (data: any) => {
              (<any>process).send(data);

              if (save_count >= 5) {
                var conn = await connectionDB();
                await conn.beginTransaction();

                try {
                  for (var i = 0; i < data.PLC_address_index.length; i++) {
                    var query =
                      "insert into tb_plc_data(plc_data_ip, plc_data_port, plc_data_address, plc_mode, plc_data) values (?, ?, ?, ?, ?);";
                    var params = [
                      data.IP,
                      data.port,
                      data.PLC_address_index[i],
                      data.Mode[i],
                      String(data[data.PLC_address_index[i]]),
                    ];
                    // conn.query(query, params)
                  }

                  // tb_sensor_data에 데이터 쌓기 성공
                  for (var i = 0; i < data.PLC_addresses.length; i++) {
                    let setting: any = {};

                    var PLC_addresses_split = data.PLC_addresses[i].toString().split("-");

                    let functionCode = PLC_addresses_split[1];
                    let address = PLC_addresses_split[2];

                    let addressIndex = Math.floor(address / 100) * 100;

                    var value = data[String(functionCode + "-" + addressIndex)][address % 100];

                    if (data.Type[i] == "bit" || data.Type[i] == "word") {
                      if (value > 60000) {
                        value = 0;
                      }

                      setting = { address: data.PLC_addresses[i], value: value, mt_idx: data.Matching_idx[i] };
                    } else if (data.Type[i] == "wtb") {
                      let wordToBit = address.split(".");
                      let binaryValue = data[String(functionCode + "-" + addressIndex)][wordToBit[0] % 100]
                        .toString(2)
                        .padStart(16, 0);

                      value = binaryValue[15 - wordToBit[1]];

                      setting = { address: data.PLC_addresses[i], value: value, mt_idx: data.Matching_idx[i] };
                    } else if (data.Type[i] == "double") {
                      dataObj = { ...dataObj, [data.PLC_addresses[i]]: data.Matching_idx[i] };
                      dataArr.push(value);

                      if (dataArr.length == 2) {
                        let highValue = dataArr[1] * 65536;
                        let rowValue = dataArr[0];

                        value = rowValue + highValue;

                        setting = { address: Object.keys(dataObj)[1], value: value, mt_idx: Object.values(dataObj)[1] };
                        dataArr = [];
                        dataObj = {};
                      }
                    }

                    if (Object.keys(setting).length > 0) {
                      if (data.Complement[i] == 1) {
                        setting.value =
                          Number(setting.value) & (1 << 15) ? Number(setting.value) - (1 << 16) : Number(setting.value);
                      }

                      if (data.PLC_formulas[i] != "" && data.PLC_formulas[i] != null) {
                        var formula = data.PLC_formulas[i];
                        var text = "";

                        if (!(setting.value == 0)) {
                          text = text.concat(setting.value, formula);
                          setting.value = eval(text);
                        }
                      }

                      if (isNaN(Number(setting.value)) == false && Number.isInteger(Number(setting.value)) == false) {
                        setting.value = parseFloat(Number(setting.value).toFixed(1));
                      }

                      var query = "insert into tb_sensor_data (sn_data_address, sn_data, mt_idx) values (?, ?, ?);";
                      var params = [setting.address, setting.value, setting.mt_idx];

                      conn.query(query, params);

                      // query = "update tb_real_time_list set sn_data = ?, updatedAt = ? where mt_idx = ?;"
                      // params = [setting.value, moment().format('YYYY-MM-DD HH:mm:ss'), setting.mt_idx]

                      // conn.query(query, params)

                      setting = {};
                    }
                  }

                  conn.commit();
                  // console.log('data commit!')
                } catch (err) {
                  conn.rollback();
                  throw err;
                } finally {
                  conn.release();
                }

                save_count = 0;
              }
            })
            .catch((err) => {
              console.log(err);
            });
        }
      }
    }, 1000);
  });

  socket.on("close", () => {
    console.log("Close IP: " + Total_PLC_Data.IP);
    console.log("Close Port: " + Total_PLC_Data.port);
    console.log(checkData);

    let options = {
      host: Total_PLC_Data.IP,
      port: Number(Total_PLC_Data.port),
    };

    refresh = setTimeout(() => {
      save_count = 0;
      clearInterval(refresh);
      socket.connect(options);
    }, 1000 * 5);
  });

  socket.on("error", () => {
    console.log("Error IP:" + Total_PLC_Data.IP);
  });

  socket.on("timeout", () => {
    console.log("Timeout IP:" + Total_PLC_Data.IP);
  });

  process.on("message", async function (message: any) {
    // console.log(message)
    if (JSON.stringify(message).includes("writeData")) {
      // console.log("워커" + process.pid + "가 마스터에게 받은 메시지 : " + message);
      let setting = message.split("/");
      setting = JSON.parse(setting[1]);
      writeObj = setting;
    } else if (message.type === "shutdown") {
    } else {
      if (message !== null || message !== undefined) {
        const options: SocketConnectOpts = {
          host: message.IP,
          port: message.port,
        };

        Total_PLC_Data = { ...Total_PLC_Data, IP: message.IP };
        Total_PLC_Data = { ...Total_PLC_Data, port: message.port };
        Total_PLC_Data = { ...Total_PLC_Data, path: message.Path };
        Total_PLC_Data = { ...Total_PLC_Data, code: message.Code };

        mclient = new client.TCP(socket, Number(message.Code));

        if (message.addresses !== undefined) {
          for (var item of message.addresses) {
            if (item.ad_function_code == "01") {
              if (item.ad_address.includes("/"))
                for (var a of item.ad_address.split("/")) PLC_addresses_01.push(Math.floor(a / 100));
              else PLC_addresses_01.push(Math.floor(item.ad_address / 100));
            } else if (item.ad_function_code == "02") {
              if (item.ad_address.includes("/"))
                for (var a of item.ad_address.split("/")) PLC_addresses_02.push(Math.floor(a / 100));
              else PLC_addresses_02.push(Math.floor(item.ad_address / 100));
            } else if (item.ad_function_code == "03") {
              if (item.ad_address.includes("/"))
                for (var a of item.ad_address.split("/")) PLC_addresses_03.push(Math.floor(a / 100));
              else {
                PLC_addresses_03.push(Math.floor(item.ad_address / 100));
              }
            } else if (item.ad_function_code == "04") {
              if (item.ad_address.includes("/"))
                for (var a of item.ad_address.split("/")) PLC_addresses_04.push(Math.floor(a / 100));
              else PLC_addresses_04.push(Math.floor(item.ad_address / 100));
            } else if (item.ad_function_code == "77") {
              if (item.ad_address.includes("/"))
                for (var a of item.ad_address.split("/")) PLC_addresses_77.push(Math.floor(a / 100));
              else PLC_addresses_77.push(Math.floor(item.ad_address / 100));
            }
          }

          PLC_addresses_01 = Array.from(new Set(PLC_addresses_01)).sort((a, b) => a - b);
          PLC_addresses_02 = Array.from(new Set(PLC_addresses_02)).sort((a, b) => a - b);
          PLC_addresses_03 = Array.from(new Set(PLC_addresses_03)).sort((a, b) => a - b);
          PLC_addresses_04 = Array.from(new Set(PLC_addresses_04)).sort((a, b) => a - b);
          PLC_addresses_77 = Array.from(new Set(PLC_addresses_77)).sort((a, b) => a - b);

          var PLC_address_index = new Array();
          var PLC_addresses = new Array();
          var PLC_formulas = new Array();
          var Facility_names = new Array();
          var Facility_groups = new Array();
          var Sensor_names = new Array();
          var Sensor_abbreviations = new Array();
          var Matching_idx = new Array();
          var Complement = new Array();
          var Mode = new Array();
          var sensorIdx = new Array();
          var Type = new Array();

          for (var i of PLC_addresses_01) PLC_address_index.push(String("01-" + i * 100));
          for (var i of PLC_addresses_02) PLC_address_index.push(String("02-" + i * 100));
          for (var i of PLC_addresses_03) PLC_address_index.push(String("03-" + i * 100));
          for (var i of PLC_addresses_04) PLC_address_index.push(String("04-" + i * 100));
          for (var i of PLC_addresses_77) PLC_address_index.push(String("77-" + i * 100));

          Total_PLC_Data = { ...Total_PLC_Data, PLC_address_index: PLC_address_index };

          for (var i of message.addresses) {
            PLC_formulas.push(i.ad_formula);
            Facility_names.push(i.fa_name);
            Facility_groups.push(i.fa_group);
            Sensor_names.push(i.sn_name);
            Sensor_abbreviations.push(i.sn_abbreviation);
            PLC_addresses.push(String(i.fa_idx + "-" + i.ad_function_code + "-" + i.ad_address));
            Matching_idx.push(i.mt_idx);
            Complement.push(i.ad_complement);
            Mode.push(i.mt_mode);
            sensorIdx.push(i.sn_idx);
            Type.push(i.ad_type);
          }
          Total_PLC_Data = { ...Total_PLC_Data, sensorIdx: sensorIdx };
          Total_PLC_Data = { ...Total_PLC_Data, PLC_formulas: PLC_formulas };
          Total_PLC_Data = { ...Total_PLC_Data, Facility_names: Facility_names };
          Total_PLC_Data = { ...Total_PLC_Data, Facility_groups: Facility_groups };
          Total_PLC_Data = { ...Total_PLC_Data, Sensor_names: Sensor_names };
          Total_PLC_Data = { ...Total_PLC_Data, Sensor_abbreviations: Sensor_abbreviations };
          Total_PLC_Data = { ...Total_PLC_Data, PLC_addresses: PLC_addresses };
          Total_PLC_Data = { ...Total_PLC_Data, Matching_idx: Matching_idx };
          Total_PLC_Data = { ...Total_PLC_Data, Complement: Complement };
          Total_PLC_Data = { ...Total_PLC_Data, Mode: Mode };
          Total_PLC_Data = { ...Total_PLC_Data, Type: Type };

          await socket.connect(options, function () {
            console.log("워커 " + process.pid + " CONNECTED TO : " + options.host + ":" + options.port);
          });
        }
      }
    }
  });
}

export let plcprogram: express.Express = express();
export default plcprogram;
