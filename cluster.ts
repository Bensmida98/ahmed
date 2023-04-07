import { Observable, BehaviorSubject } from "rxjs";

import { trace, nivTrace } from "../util/logger";
import { MONGODB_URI_CONF, MONGODB_URI_SYST } from "../util/mongo";

import { sendAlarmeNue } from "../controllers/supervision";
const Supervision = require("@palladium/supervision");

const MongoClient = require("mongodb").MongoClient;
let Maindb: any;
const maxTry = 3;

// Le composant pour trace à destination ELK
const composant = "ClusterMed";
// -------------------------------------------------------------------------
//  Fonction d'arret forcé après temporisation
//  la fonction boucle sur elle-meme (toutes les secondes) jusqu'a :
//      - 'plus de workers actifs'
//          ou
//      - atteinte tempo
function stopLimit(workers: any, i: number, timeOut: number) {
  if (i < timeOut) {
    // timeOut non atteint, on regarde si il reste des workers
    if (workers.length > 0) {
      // il y a encore des workers, on continue a temporiser
      setTimeout(() => {
        stopLimit(workers, i + 1, timeOut);
      }, 1000);
    }
    // si il n'y a plus de worker, la fonction s'arrete
  } else {
    // timeOut atteint, on force l'arret des workers encore en cours
    let k, len;
    for (k = 0, len = workers.length; k < len; k++) {
      const worker = workers[k];
      console.log("Demande arret SIGINT sur pid " + worker.process.pid);
      process.kill(worker.process.pid, "SIGINT");
    }
  }
}

// -------------------------------------------------------------------------------------------

function savePid(wId: number, pId: number): Observable<string> {
  const funcname = "savePid";
  const state = new BehaviorSubject("init");

  const myArt = <any>{};
  myArt.wId = wId;
  myArt.pId = pId;

  try {
    const optionsURI = {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      minPoolSize: 1,
      maxPoolSize: 1,
    };

    MongoClient.connect(
      MONGODB_URI_SYST,
      optionsURI,
      function (err: any, client: any) {
        if (err) {
          trace(
            composant,
            funcname,
            nivTrace.error,
            "erreur connect clusterMed 1"
          );
          // sendAlarme( 10200, '', '', '', '', Date.now(), undefined, process.env.DATABASE );
          state.error({ wId: wId, pId: pId, err: err });
          return state;
        } else {
          Maindb = client.db(process.env.DATABASE);

          Maindb.collection("wId")
            .findOneAndUpdate(
              {
                worker: wId,
                serveur: process.env.NOM_SERVER,
                module: process.env.NOM_MODULE,
              },
              { $set: { pId: pId } },
              { upsert: true }
            )
            .then((res: any) => {
              if (res.lastErrorObject.n === 1) {
                console.log("save : wId (" + wId + ") / pId (" + pId + ") OK");
                client.close();
                state.complete();
                return state;
              } else {
                console.log("Erreur de sauvegarde wId");
                client.close();
                state.error({
                  wId: wId,
                  pId: pId,
                  err: "Erreur de sauvegarde wId",
                });
                return state;
              }
            });
        }
      }
    );
  } catch (err) {
    trace(composant, funcname, nivTrace.error, "erreur connect clusterMed 1.1");
    // sendAlarme( 10200, '', '', '', '', Date.now(), undefined, process.env.DATABASE );
    state.error(err);
    return state;
  }
  return state;
}

function updatePid(pId: number, new_pId: number): Observable<string> {
  const funcname = "updatePid";
  const state = new BehaviorSubject("init");

  const optionsURI = {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    minPoolSize: 1,
    maxPoolSize: 1,
  };

  MongoClient.connect(
    MONGODB_URI_SYST,
    optionsURI,
    function (err: any, client: any) {
      if (err) {
        trace(
          composant,
          funcname,
          nivTrace.error,
          "erreur connect clusterMed 2"
        );
        state.error(err);
        return state;
      }

      Maindb = client.db(process.env.DATABASE);

      Maindb.collection("wId")
        .findOneAndUpdate(
          {
            pId: pId,
            serveur: process.env.NOM_SERVER,
            module: process.env.NOM_MODULE,
          },
          { $set: { pId: new_pId } }
        )
        .then((res: any) => {
          if (res === undefined) {
            console.log("Erreur de mise a jour pId");
            client.close();
            state.error("Erreur de mise a jour pId");
            return state;
          }

          if (res.lastErrorObject.n === 1) {
            console.log(
              "update : pId (" + pId + ") -> pId (" + new_pId + ") OK"
            );
            client.close();
            state.complete();
            return state;
          } else {
            console.log("Erreur de mise a jour pId");
            client.close();
            state.error("Erreur de mise a jour pId");
            return state;
          }
        });
    }
  );
  return state;
}

export function getOldWid(pId: number) {
  const funcname = "getOldWid";
  return new Promise(function (resolve, reject) {
    const optionsURI = {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      minPoolSize: 1,
      maxPoolSize: 1,
    };

    MongoClient.connect(
      MONGODB_URI_SYST,
      optionsURI,
      function (err: any, client: any) {
        if (err) {
          trace(
            composant,
            funcname,
            nivTrace.error,
            "erreur connect clusterMed 3"
          );
          reject(err);
        }

        Maindb = client.db(process.env.DATABASE);

        readKey(pId, 1)
          .then(function (oldKey) {
            client.close();
            resolve(oldKey);
          })
          .catch((err) => {
            client.close();
            reject(err);
          });
      }
    );
  });
}

function readKey(key: number, iTry: number) {
  return new Promise(function (resolve, reject) {
    Maindb.collection("wId").findOne(
      {
        pId: key,
        serveur: process.env.NOM_SERVER,
        module: process.env.NOM_MODULE,
      },
      function (err: any, res: any) {
        if (err) {
          console.log(err);
          reject("<wId> collection not readable");
        } else {
          if (res === null) {
            if (iTry >= maxTry) {
              reject("clé pId non trouvée dans wId");
            } else {
              setTimeout(() => {
                readKey(key, iTry + 1)
                  .then(function (oldKey) {
                    console.log("resolve ", oldKey);
                    resolve(oldKey);
                  })
                  .catch((err) => {
                    reject(err);
                  });
              }, 3000);
            }
          } else {
            resolve(res.worker);
          }
        }
      }
    );
  });
}

function updateNbrProcesseur(
  /* nomMachine: any, nommodule: any, */ nbrProcesseur: number,
  listWid: any
): Observable<string> {
  const funcname = "updateNbrProcesseur";
  const state = new BehaviorSubject("init");
  let machineStatus: string;
  nbrProcesseur > 0 ? (machineStatus = "started") : (machineStatus = "stopped");

  const optionsURI = {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    minPoolSize: 1,
    maxPoolSize: 1,
  };

  MongoClient.connect(
    MONGODB_URI_CONF,
    optionsURI,
    function (err: any, client: any) {
      if (err) {
        trace(composant, funcname, nivTrace.error, "erreur connect clusterMed");
        state.error(err);
        return state;
      }

      Maindb = client.db(process.env.CONFBASE);

      Maindb.collection("machinesPalladium")
        .findOneAndUpdate(
          {
            nom: process.env.NOM_SERVER,
            module: process.env.NOM_MODULE,
            adresse: process.env.ADRESSE_IP,
          },
          {
            $set: {
              statut: machineStatus,
              nbrProcesseur: nbrProcesseur,
              Process: listWid,
            },
          },
          { upsert: true }
        )
        .then((res: any) => {
          if (res === undefined) {
            console.log("Erreur de mise a jour nbrProcesseur");
            client.close();
            state.error("Erreur de mise a jour nbrProcesseur");
            return state;
          }

          if (res.lastErrorObject.n === 1) {
            console.log("update : nbrProcesseur= " + nbrProcesseur + " OK");
            client.close();
            state.complete();
            return state;
          } else {
            console.log("Erreur de mise a jour nbrProcesseur");
            client.close();
            state.error("Erreur de mise a jour nbrProcesseur");
            return state;
          }
        });
    }
  );
  return state;
}

function decreaseNbrProcesseur(
  nbrToDecrease: number,
  arg: any
): Observable<string> {
  const funcname = "decreaseNbrProcesseur";
  const state = new BehaviorSubject("init");

  const optionsURI = {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    minPoolSize: 1,
    maxPoolSize: 1,
  };

  MongoClient.connect(
    MONGODB_URI_CONF,
    optionsURI,
    function (err: any, client: any) {
      if (err) {
        trace(composant, funcname, nivTrace.error, "erreur connect clusterMed");
        state.error(err);
        return state;
      }

      Maindb = client.db(process.env.CONFBASE);

      Maindb.collection("machinesPalladium").findOneAndUpdate(
        {
          nom: process.env.NOM_SERVER,
          module: process.env.NOM_MODULE,
          adresse: process.env.ADRESSE_IP,
        },
        { $inc: { nbrProcesseur: -1 }, $pull: { Process: arg.workerId } },
        { returnDocument: "before" },
        function (err: any, res: any) {
          if (err) {
            console.log("Erreur de mise a jour nbrProcesseur");
            client.close();
            state.error("Erreur de mise a jour nbrProcesseur");
            return state;
          }
          if (res.lastErrorObject.n === 1) {
            console.log(
              "update machinesPalladium OK, nbrProcesseur: ancien=",
              res.value.nbrProcesseur,
              " courant=",
              res.value.nbrProcesseur - 1
            );
          }
          if (res.value.nbrProcesseur > 1) {
            client.close();
            state.complete();
            return state;
          } else if (res.value.nbrProcesseur === 1) {
            Maindb.collection("machinesPalladium")
              .updateOne(
                {
                  nom: process.env.NOM_SERVER,
                  module: process.env.NOM_MODULE,
                  adresse: process.env.ADRESSE_IP,
                },
                { $set: { statut: "stopped" } }
              )
              .then((res: any, err: any) => {
                if (err) {
                  trace(composant, funcname, nivTrace.error, err);
                  console.log("error Erreur de mise a jour statut");
                  client.close();
                  state.error("Erreur de mise a jour statut");
                  return state;
                }
                if (res) {
                  console.log("update machinesPalladium OK - statut stopped"); // , nbrProcesseur=', res.value.nbrProcesseur);
                  client.close();
                  state.complete();
                  return state;
                }
              });
          }
        }
      );
    }
  );
  return state;
}

function initNbrProcesseur(): Observable<string> {
  const funcname = "initNbrProcesseur";
  const state = new BehaviorSubject("init");

  const optionsURI = {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    minPoolSize: 1,
    maxPoolSize: 1,
  };

  MongoClient.connect(
    MONGODB_URI_CONF,
    optionsURI,
    function (err: any, client: any) {
      if (err) {
        trace(composant, funcname, nivTrace.error, "erreur connect clusterMed");
        state.error(err);
        return state;
      }

      Maindb = client.db(process.env.CONFBASE);

      Maindb.collection("machinesPalladium")
        .findOneAndUpdate(
          {
            nom: process.env.NOM_SERVER,
            module: process.env.NOM_MODULE,
            adresse: process.env.ADRESSE_IP,
          },
          { $set: { statut: "started", nbrProcesseur: 0, Process: {} } },
          { upsert: true }
        )
        .then((res: any) => {
          if (res === undefined) {
            console.log("Erreur de mise a jour nbrProcesseur");
            client.close();
            state.error("Erreur de mise a jour nbrProcesseur");
            return state;
          }

          if (res.lastErrorObject.n === 1) {
            client.close();
            state.complete();
            return state;
          } else {
            console.log("Erreur de mise a jour nbrProcesseur");
            client.close();
            state.error("Erreur de mise a jour nbrProcesseur");
            return state;
          }
        });
    }
  );
  return state;
}

export function myCluster(arg0: any, arg1: any) {
  const funcname = "myCluster";
  const myClusterFct = function () {
    const cluster = require("cluster");

    const master = function (config: any) {
      const supervision = new Supervision(
        process.env.ELK_ELASTIC_URL,
        process.env.MONGODB_URI_CONF,
        process.env.ELK_CHEMIN_ECRITURE_DISQUE,
        1,
        process.env.ELK_LIB_MODE_DEBUG ? process.env.ELK_LIB_MODE_DEBUG : ""
      );

      const worker_map = new Map();

      initNbrProcesseur()
        .toPromise()
        .then(() => {
          let i, j, ref;
          let worker: any;
          const count = parseInt(config.count || process.env.WORKER_COUNT);
          const workerCount = count > 0 ? count : 1;
          let respawn =
            typeof config.respawn === "undefined"
              ? true
              : Boolean(config.respawn);
          const outputStream =
            config.outputStream &&
            typeof config.outputStream.write === "function"
              ? config.outputStream.write
              : console.log;
          const workers = <any>[];
          if (config.verbose) {
            outputStream(
              "Master started on pid " +
                process.pid +
                ", forking " +
                workerCount +
                " processes"
            );
          }
          const listWid = <any>[];
          for (
            i = j = 0, ref = workerCount;
            0 <= ref ? j < ref : j > ref;
            i = 0 <= ref ? ++j : --j
          ) {
            worker = cluster.fork();

            // Mémorisation workerId/nbRestart dans la map worker_map
            worker_map.set(worker.id, { workerId: worker.id, restart: 0 });
            // listWid.push(process.env.NOM_SERVER + '_' + worker.id.toString().padStart(2, '0'));
            listWid.push(worker.id);

            if (typeof config.workerListener === "function") {
              worker.on("message", config.workerListener);
            }
            workers.push(worker);
            savePid(worker.id, worker.process.pid)
              .toPromise()
              .then(() => {
                console.log("save wId OK");
              })
              .catch((err: any) => {
                trace(
                  composant,
                  funcname,
                  nivTrace.error,
                  "(wId " + err.wId + ") " + "clusterMed : error save pid"
                );
              });
          }

          // Mettre à jour la collection machinesPalladium
          updateNbrProcesseur(workers.length, listWid)
            .toPromise()
            .then(() => {
              console.log(
                "update machinesPalladium, nbrProcesseur(" +
                  workers.length +
                  ")"
              );
            })
            .catch((err) => {
              trace(
                composant,
                funcname,
                nivTrace.error,
                "Erreur de mise a jour collection machinesPalladium, err = " +
                  err
              );
            });

          cluster.on("exit", function (worker: any, code: any, signal: any) {
            let idx;
            let restart = 0;
            if (respawn) restart = 1;
            if (code === 99) restart = 0;
            if (config.verbose) {
              outputStream(
                worker.process.pid +
                  " died with " +
                  (signal || "exit code " + code) +
                  (restart ? ", restarting" : "")
              );
            }
            outputStream(
              worker.process.pid +
                " died with signal " +
                signal +
                " , exit code " +
                code +
                (restart ? ", restarting" : "")
            );
            idx = workers.indexOf(worker);
            let oldId = 0;
            let oldPid = 0;
            if (idx > -1) {
              oldPid = worker.process.pid;
              oldId = workers[idx].id;
              workers.splice(idx, 1);
            }
            // if (respawn) {
            if (restart) {
              // on récupère les infos du worker qui est tombé
              const env = worker_map.get(worker.id);
              // incrémentation du nombre de reboots
              env.restart++;
              // suppression ancien worker
              worker_map.delete(worker.id);
              worker = cluster.fork(env); // les infos worker initial/nb reboots sont transmis au démarrage du nouveau worker
              // mise a jour nouveau worker
              worker_map.set(worker.id, env);

              if (idx > -1) {
                outputStream(
                  oldPid +
                    " died on workerId " +
                    oldId +
                    " , respawn with " +
                    worker.process.pid +
                    " on workerId " +
                    worker.id
                );
                console.log(
                  "respawn workerId :",
                  worker.id,
                  "/",
                  worker.process.pid
                );
                updatePid(oldPid, worker.process.pid)
                  .toPromise()
                  .then(() => {
                    console.log("update wId OK");
                  })
                  .catch((err) => {
                    trace(
                      composant,
                      funcname,
                      nivTrace.error,
                      "Erreur de mise a jour collection wId, err = " + err
                    );
                  });
              }

              if (typeof config.workerListener === "function") {
                worker.on("message", config.workerListener);
              }
              worker.oldId = oldId;
              const myWorker = workers.push(worker);
              let i = 0;
              for (i = 0; i < workers.length; i++) {
                console.log(
                  "respawn i=",
                  i,
                  ":",
                  workers[i].id,
                  "/",
                  workers[i].process.pid
                );
              }
              return myWorker;
            } else {
              decreaseNbrProcesseur(1, worker_map.get(worker.id))
                .toPromise()
                .then(() => {
                  console.log(
                    "update machinesPalladium, decrease nbrProcesseur OK"
                  );
                  if (workers.length === 0) {
                    if (code === 99) {
                      // Ce n'est pas un arret propre, on emet une alarme

                      const alarmeElk = {
                        alarmeId: 90112,
                        date: Date.now(),
                        etat: 0,
                        source: process.env.NOM_MODULE,
                        classe: "M",
                        module: "Palladium",
                        urgence: "AII",
                        typeSrc: "MODULE",
                        dest: "J",
                        libelle: `- Arret du module ${process.env.NOM_MODULE} sur le serveur ${process.env.NOM_SERVER}`,
                      };
                      sendAlarmeNue(alarmeElk);
                    }

                    console.log("Plus de worker actif, arret du master ......");
                    setTimeout(() => {
                      // On attend quelques secondes pour etre sur que toutes les connexions sont fermees
                      process.exit();
                    }, 5000);
                  }
                })
                .catch((err) => {
                  trace(
                    composant,
                    funcname,
                    nivTrace.error,
                    "Erreur de mise a jour collection machinesPalladium"
                  );
                  console.log("err =", err);
                });
            }
          });

          process.on("SIGTERM", function () {
            let k, len, results;
            respawn = false;
            if (config.verbose) {
              outputStream(
                "TERM received, will exit once all workers have finished current requests"
              );
            }
            results = [];
            for (k = 0, len = workers.length; k < len; k++) {
              worker = workers[k];
              results.push(worker.send("quit"));
              // Activation arret propre
              process.kill(worker.process.pid, "SIGTERM");
            }
            // Activation arret forcé après temporisation (de STOP_DELAY secondes, max 120)
            const Delay = Number(
              process.env.STOP_DELAY ? process.env.STOP_DELAY : 120
            );
            stopLimit(workers, 0, Delay);
            return results;
          });

          return process.on("SIGQUIT", function () {
            let k, len, results;
            respawn = false;
            if (config.verbose) {
              outputStream(
                "QUIT received, will exit once all workers have finished current requests"
              );
            }
            results = [];
            for (k = 0, len = workers.length; k < len; k++) {
              worker = workers[k];
              results.push(worker.send("quit"));
            }
            return results;
          });
        })
        .catch((err: any) => {
          trace(
            composant,
            funcname,
            nivTrace.error,
            "Erreur de mise a jour collection machinesPalladium"
          );
          console.log("err =", err);
          return [];
        });
    };

    const worker = function (fn: any, worker: any) {
      const server = fn(worker);
      if (!server) {
        return;
      }
      if (typeof server.on === "function") {
        server.on("close", function () {
          return process.exit();
        });
      }
      if (typeof server.close === "function") {
        return process.on("message", function (msg) {
          if (msg === "quit") {
            return server.close();
          }
        });
      }
    };

    const myFunctCluster = function (arg0: any, arg1: any) {
      let config, fn;
      fn = function () {};
      config = {};
      if (typeof arg0 === "function") {
        fn = arg0;
        config = arg1 || config;
      } else if (typeof arg1 === "function") {
        fn = arg1;
        config = arg0 || config;
      }

      if (cluster.isMaster) {
        return master(config);
      } else {
        return worker(fn, cluster.worker);
      }
    };

    return myFunctCluster(arg0, arg1);
  }.call(this, arg0, arg1);

  return myClusterFct;
}
