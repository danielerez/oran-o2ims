package service

import (
	"bytes"
	"context"
	"log/slog"

	"github.com/openshift-kni/oran-o2ims/internal/data"
	"github.com/openshift-kni/oran-o2ims/internal/jq"
)

// Add is the implementation of the object handler ADD interface.
// receive obsability alarm post and trigger the alarms

func (h *alarmNotificationHandler) Add(ctx context.Context,
	request *AddRequest) (response *AddResponse, err error) {

	h.logger.Debug(
		"alarmNotificationHandler Add",
	)

	var eventRecordId string
	err = h.jqTool.Evaluate(`.alarmEventRecordId`, request.Object, &eventRecordId)
	if err != nil {
		h.logger.Debug(
			"alarm does not have alarmEventRecordId included ",
		)
	}

	subIdSet := h.getSubscriptionIdsFromAlarm(ctx, request.Object)

	//now look up subscriptions id_set matched and send http packets to URIs
	for key := range subIdSet {
		subInfo, ok := h.getSubscriptionInfo(ctx, key)

		if !ok {
			h.logger.Debug(
				"alarmNotificationHandler failed to get subinfo key",
				slog.String(": ", key),
			)

			continue
		}

		var obj data.Object
		// TODO:
		// determin if alarmNotificationType needs to be added

		err = h.jqTool.Evaluate(
			`{
				"alarmSubscriptionId": $subId,
				"consumerSubscriptionId": $alarmConsumerSubId,
				"objectRef": $objRef,
				"alarmEventRecord": $alarmEvent
			}`,
			request.Object, &obj,
			jq.String("$alarmConsumerSubId", subInfo.consumerSubscriptionId),
			jq.String("$objRef", eventRecordId),
			jq.String("$subId", key),
			jq.Any("$alarmEvent", request.Object),
		)

		//following function will send out the notification packet based on subscription
		//and alert received in a go route thread that will not hold the AddRequest ctx.
		go func(pkt data.Object) {
			content, err := h.jsonAPI.MarshalIndent(&pkt, "", " ")
			if err != nil {
				h.logger.Debug(
					"alarmNotificationHandler failed to get content of new packet",
					slog.String("error", err.Error()),
				)
			}

			//following new buffer usage may need optimization
			resp, err := h.httpClient.Post(subInfo.uris, "application/json", bytes.NewBuffer(content))
			if err != nil {
				h.logger.Debug("alarmNotificationHandler failed to post packet",
					slog.String("error", err.Error()),
				)
				return
			}

			defer resp.Body.Close()
		}(obj)

	}

	response = &AddResponse{}
	return
}
