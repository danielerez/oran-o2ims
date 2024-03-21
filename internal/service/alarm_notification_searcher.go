/*
Copyright 2023 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is
distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations under the
License.
*/

package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/openshift-kni/oran-o2ims/internal/data"
	"github.com/openshift-kni/oran-o2ims/internal/jq"
	"github.com/openshift-kni/oran-o2ims/internal/search"
)

type subscriptionInfo struct {
	filters                search.Selector
	uris                   string
	consumerSubscriptionId string
}

// This file contains oran alarm notification serer search for matched subscriptions
// at 1st step apply linear search
type alarmSubscriptionSearcher struct {
	logger *slog.Logger
	jqTool *jq.Tool
	//maps with prebuilt selector
	subscriptionInfoMap *map[string]subscriptionInfo
	pathIndexMap        *map[string]alarmSubIdSet
	noFilterSubsSet     *alarmSubIdSet

	//Parser used for the subscription filters
	selectorParser *search.SelectorParser
}

func (b *alarmSubscriptionSearcher) SetLogger(
	value *slog.Logger) *alarmSubscriptionSearcher {
	b.logger = value
	return b
}

func (b *alarmSubscriptionSearcher) SetJqTool(
	value *jq.Tool) *alarmSubscriptionSearcher {
	b.jqTool = value
	return b
}

func (b *alarmSubscriptionSearcher) SetSelectorParser(
	parser *search.SelectorParser) *alarmSubscriptionSearcher {
	b.selectorParser = parser
	return b
}

func newAlarmSubscriptionSearcher() *alarmSubscriptionSearcher {
	return &alarmSubscriptionSearcher{}
}

func (b *alarmSubscriptionSearcher) build() {
	b.subscriptionInfoMap = &map[string]subscriptionInfo{}
	b.pathIndexMap = &map[string]alarmSubIdSet{}
	b.noFilterSubsSet = &alarmSubIdSet{}

	// Create the filter expression parser:
	selectorParser, err := search.NewSelectorParser().
		SetLogger(b.logger).
		Build()
	if err != nil {
		b.logger.Error(
			"failed to create filter expression parser: ",
			slog.String("error: ", err.Error()),
		)
		return
	}
	b.selectorParser = selectorParser
}

// NOTE the function should be called by a function that is holding the semophone
func (b *alarmSubscriptionSearcher) getSubFilters(filterStr string, subId string) (err error) {

	//no filter found, return empty array and behavior as "*"
	if filterStr == "" {
		(*b.noFilterSubsSet)[subId] = struct{}{}
		return
	}

	result, err := b.selectorParser.Parse(filterStr)

	if err != nil {
		return
	}

	subInfo := (*b.subscriptionInfoMap)[subId]
	subInfo.filters = *result
	(*b.subscriptionInfoMap)[subId] = subInfo

	//for now use path 0 only
	//to be fixed with full path for quicker search
	for _, element := range result.Terms {
		_, ok := (*b.pathIndexMap)[element.Path[0]]

		if !ok {
			(*b.pathIndexMap)[element.Path[0]] = alarmSubIdSet{}
		}
		(*b.pathIndexMap)[element.Path[0]][subId] = struct{}{}
	}

	return
}

// NOTE the function should be called by a function that is holding the semophone
func (b *alarmSubscriptionSearcher) pocessSubscriptionMapForSearcher(subscriptionMap *map[string]data.Object,
	jqTool *jq.Tool) (err error) {

	for key, value := range *subscriptionMap {

		(*b.subscriptionInfoMap)[key] = subscriptionInfo{}

		subInfo := (*b.subscriptionInfoMap)[key]
		//get uris
		var uris string
		err = jqTool.Evaluate(`.callback`, value, &uris)
		if err != nil {
			panic("the callback is mandary for subscription")
		}
		subInfo.uris = uris

		var consumerId string
		err = jqTool.Evaluate(`.consumerSubscriptionId`, value, &consumerId)
		if err == nil {
			subInfo.consumerSubscriptionId = consumerId
		}

		(*b.subscriptionInfoMap)[key] = subInfo

		//get filter from data object
		var filter string
		err = jqTool.Evaluate(`.filter`, value, &filter)
		if err != nil {
			b.logger.Debug(
				"Subscription  ", key,
				" does not have filter included",
			)
		}

		err = b.getSubFilters(filter, key)
		if err != nil {
			b.logger.Debug(
				"pocessSubscriptionMapForSearcher ",
				"subscription: ", key,
				" error: ", err.Error(),
			)
		}
	}
	return
}

func (h *alarmNotificationHandler) getSubscriptionIdsFromAlarm(ctx context.Context, alarm data.Object) (result alarmSubIdSet) {

	h.subscriptionMapMemoryLock.RLock()
	defer h.subscriptionMapMemoryLock.RUnlock()
	result = *h.subscriptionSearcher.noFilterSubsSet

	subCheckedSet := alarmSubIdSet{}

	for path, subSet := range *h.subscriptionSearcher.pathIndexMap {

		var alarmPath string
		path = fmt.Sprintf(`.%s`, path)
		err := h.jqTool.Evaluate(path, alarm, &alarmPath)

		if err == nil {
			for subId := range subSet {

				_, ok := result[subId]
				if ok {
					continue
				}

				_, ok = subCheckedSet[subId]
				if ok {
					continue
				}
				subCheckedSet[subId] = struct{}{}

				subInfo := (*h.subscriptionSearcher.subscriptionInfoMap)[subId]
				match, err := h.selectorEvaluator.Evaluate(ctx, &subInfo.filters, alarm)
				if err != nil {
					h.logger.Debug(
						"pocessSubscriptionMapForSearcher ",
						"subscription: ", subId,
						" error: ", err.Error(),
					)
					continue
				}
				if match {
					h.logger.Debug(
						"pocessSubscriptionMapForSearcher MATCH ",
						"subscription: ", subId,
					)
					result[subId] = struct{}{}
				}
			}
		}
	}

	return
}

func (h *alarmNotificationHandler) getSubscriptionInfo(ctx context.Context, subId string) (result subscriptionInfo, ok bool) {
	h.subscriptionMapMemoryLock.RLock()
	defer h.subscriptionMapMemoryLock.RUnlock()
	result, ok = (*h.subscriptionSearcher.subscriptionInfoMap)[subId]
	return
}
