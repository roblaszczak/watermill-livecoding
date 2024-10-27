package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/lmittmann/tint"
)

type BookRoomRequest struct {
	RoomID      string `json:"room_id"`
	GuestsCount int    `json:"guests_count"`
}

type RoomBookingHandler struct {
	payments PaymentsProvider
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

	roomPrice := 42 * req.GuestsCount

	err = h.payments.TakePayment(roomPrice)
	if err != nil {
		slog.With("err", err).Error("Failed to take payment")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
}

type PaymentsProvider struct{}

func (p PaymentsProvider) TakePayment(amount int) error {
	logger := slog.With("amount", amount)

	logger.Info("Taking payment")

	// this is not the best payment provider...
	if rand.Int31n(2) == 0 {
		time.Sleep(time.Second * 5)
	}
	if rand.Int31n(3) == 0 {
		return errors.New("random error")
	}

	logger.Info("Payment taken")

	return nil
}

func main() {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level:      slog.LevelInfo,
			TimeFormat: time.Kitchen,
		}),
	))

	slog.Info("Starting app")

	h := RoomBookingHandler{
		payments: PaymentsProvider{},
	}

	http.HandleFunc("POST /book", h.Handler)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)
		<-c
		cancel()
	}()

	runHTTP(ctx)
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
