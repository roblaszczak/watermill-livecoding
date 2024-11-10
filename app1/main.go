package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/lmittmann/tint"
)

type BookRoomRequest struct {
	RoomID      string `json:"room_id"`
	GuestsCount int    `json:"guests_count"`
}

type RoomBookingHandler struct {
	payments PaymentsProvider

	eventBus *cqrs.EventBus
}

type RoomBooked struct {
	BookingID   string `json:"booking_id"`
	RoomID      string `json:"room_id"`
	GuestsCount int    `json:"guests_count"`
	Price       int    `json:"price"`
}

func (h RoomBookingHandler) Handler(writer http.ResponseWriter, request *http.Request) {
	b, err := io.ReadAll(request.Body)
	if err != nil {
		slog.With("err", err).Error("Failed to read request body")
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	req := BookRoomRequest{}
	err = json.Unmarshal(b, &req)
	if err != nil {
		slog.With("err", err).Error("Failed to unmarshal request")
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	slog.With("req", req).Info("Booking room")

	bookingID := uuid.NewString()
	roomPrice := 42 * req.GuestsCount

	rb := RoomBooked{
		BookingID:   bookingID,
		RoomID:      req.RoomID,
		GuestsCount: req.GuestsCount,
		Price:       roomPrice,
	}

	err = h.eventBus.Publish(request.Context(), rb)
	if err != nil {
		slog.With("err", err).Error("Failed to publish room booked event")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
}

type PaymentsProvider struct{}

func (p PaymentsProvider) TakePayment(bookingID string, amount int) error {
	logger := slog.With("amount", amount, "booking_id", bookingID)

	logger.Info("Taking payment")

	// this is not the best payment provider...
	if rand.Int31n(2) == 0 {
		time.Sleep(time.Second * 3)
	}
	if rand.Int31n(3) == 0 {
		return errors.New("random error")
	}

	logger.Info("Payment taken")

	return nil
}

type PaymentsHandler struct {
	paymentsProvider PaymentsProvider
	eventBus         *cqrs.EventBus
}

type PaymentTaken struct {
	BookingID string `json:"booking_id"`
	RoomID    string `json:"room_id"`
	Price     int    `json:"price"`
}

func (p PaymentsHandler) Handler(ctx context.Context, rb *RoomBooked) (err error) {
	if err := p.paymentsProvider.TakePayment(rb.BookingID, rb.Price); err != nil {
		return err
	}

	return p.eventBus.Publish(ctx, PaymentTaken{
		BookingID: rb.BookingID,
		RoomID:    rb.RoomID,
		Price:     rb.Price,
	})
}

func main() {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level:      slog.LevelInfo,
			TimeFormat: time.Kitchen,
		}),
	))

	watermillLogger := watermill.NewSlogLoggerWithLevelMapping(
		slog.With("watermill", true),
		map[slog.Level]slog.Level{
			slog.LevelInfo: slog.LevelDebug,
		},
	)

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   []string{"kafka:9092"},
			Marshaler: kafka.DefaultMarshaler{},
		},
		watermillLogger,
	)
	if err != nil {
		panic(err)
	}

	slog.Info("Starting app")

	router := message.NewDefaultRouter(watermillLogger)

	marshaler := cqrs.JSONMarshaler{
		GenerateName: cqrs.StructName,
	}

	eventBus, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			return params.EventName, nil
		},
		Marshaler: marshaler,
		Logger:    watermillLogger,
	})
	if err != nil {
		panic(err)
	}

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(router, cqrs.EventProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
			return params.EventName, nil
		},
		SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return kafka.NewSubscriber(
				kafka.SubscriberConfig{
					Brokers:       []string{"kafka:9092"},
					ConsumerGroup: params.HandlerName,
					Unmarshaler:   kafka.DefaultMarshaler{},
				},
				watermillLogger,
			)
		},
		Marshaler: marshaler,
		Logger:    watermillLogger,
	})
	if err != nil {
		panic(err)
	}

	h := RoomBookingHandler{
		payments: PaymentsProvider{},
		eventBus: eventBus,
	}

	paymentsHandler := PaymentsHandler{
		eventBus: eventBus,
	}

	err = eventProcessor.AddHandlers(
		cqrs.NewEventHandler("payments", paymentsHandler.Handler),
		cqrs.NewEventHandler("payments_report", func(ctx context.Context, event *PaymentTaken) error {
			fmt.Printf("Reporting payment taken: %#v\n", event)
			return nil
		}),
		cqrs.NewEventHandler("payments_report_v2", func(ctx context.Context, event *PaymentTaken) error {
			fmt.Printf("Reporting payment taken (v2): %#v\n", event)
			return nil
		}),
	)
	if err != nil {
		panic(err)
	}

	http.HandleFunc("POST /book", h.Handler)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)
		<-c
		cancel()
	}()

	go func() {
		err := router.Run(context.Background())
		if err != nil {
			slog.With("err", err).Error("Failed to start watermill router")
		}
	}()

	runHTTP(ctx)

	err = router.Close()
	if err != nil {
		slog.With("err", err).Warn("Failed to close Watermill router")
	}
}

func runHTTP(ctx context.Context) {
	slog.Info("Running HTTP server")
	server := &http.Server{Addr: ":8080", Handler: http.DefaultServeMux}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()

	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
