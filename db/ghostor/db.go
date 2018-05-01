// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package ghostor

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/immesys/bw2/crypto"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/ucbrise/ghostor/core"
	"github.com/ucbrise/ghostor/ghostor"
	"github.com/ucbrise/ghostor/grpcint"
)

const ServerAddr = "10.0.0.176:49563"
const VerifierAddr = "localhost:14247"

var TLSConfig = &tls.Config{InsecureSkipVerify: true}

type contextKey string

const stateKey = contextKey("ghostorDB")

type ghostorDB struct {
	client   *ghostor.Client
	tokens   []*grpcint.UnblindedToken
	tokenIdx int32
}

func (db *ghostorDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *ghostorDB) CleanupThread(_ context.Context) {
}

func (db *ghostorDB) Close() error {
	return nil
}

func nameFromKey(key string) (psk []byte, pvk []byte) {
	psk = core.Hash([]byte(key))
	pvk = crypto.VKforSK(psk)
	return
}

func (db *ghostorDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	_, pvk := nameFromKey(key)
	name := crypto.FmtKey(pvk)
	data, err := db.client.ReadFile(ctx, name)
	if err == ghostor.ErrFileDoesNotExist {
		fmt.Println("Does not exist (read)", key)
		return nil, nil
	} else if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var parsed map[string][]byte
	json.Unmarshal(data, &parsed)

	var res map[string][]byte
	if len(fields) > 0 {
		res = make(map[string][]byte)
		for _, f := range fields {
			res[f] = parsed[f]
		}
	} else {
		res = parsed
	}
	return res, nil
}

func (db *ghostorDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	panic("Ghostor does not support 'Scan' operation")
}

func (db *ghostorDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	contents, err := json.Marshal(values)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, pvk := nameFromKey(key)
	name := crypto.FmtKey(pvk)
	err = db.client.WriteFile(ctx, name, contents)
	if err == ghostor.ErrFileDoesNotExist {
		fmt.Println("Does not exist (update)")
		return nil
	}
	return err
}

func (db *ghostorDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	contents, err := json.Marshal(values)
	if err != nil {
		return err
	}

	psk, pvk := nameFromKey(key)
	filename := crypto.FmtKey(pvk)

	token := db.tokens[atomic.AddInt32(&db.tokenIdx, 1)]
	if err = db.client.ChangePermissions(ctx, filename, psk, []*ghostor.Permission{}, false, token); err != nil {
		fmt.Println(err)
		return err
	}

	err = db.client.WriteFile(ctx, filename, contents)
	if err != nil {
		fmt.Println(err)
	}

	return err
}

func (db *ghostorDB) Delete(ctx context.Context, table string, key string) error {
	panic("ghostor does not support delete")
}

type ghostorCreator struct{}

func (ghostorCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	db := new(ghostorDB)

	var err error
	db.client, err = ghostor.NewClient(ServerAddr, VerifierAddr, TLSConfig)
	if err != nil {
		return nil, err
	}

	var tokenBuffer []byte
	if tokenBuffer, err = ioutil.ReadFile("tokens"); err != nil {
		fmt.Println("No \"tokens\" file found; not using tokens")
	} else {
		fmt.Println("Loading tokens from file...")
		var i uint32
		for i != uint32(len(tokenBuffer)) {
			tokenLen := binary.LittleEndian.Uint32(tokenBuffer[i : i+4])
			tokenBytes := tokenBuffer[i+4 : i+4+tokenLen]
			token := new(grpcint.UnblindedToken)
			if err = proto.Unmarshal(tokenBytes, token); err != nil {
				panic(err)
			}
			db.tokens = append(db.tokens, token)
			i += (4 + tokenLen)
		}
		db.tokenIdx = -1
		fmt.Println("Finished loading tokens")
	}

	return db, nil
}

func init() {
	ycsb.RegisterDBCreator("ghostor", ghostorCreator{})
}
