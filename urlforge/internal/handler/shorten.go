package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sde/urlforge/internal/model"
	"github.com/sde/urlforge/internal/service"
)

// ShortenHandler processes URL creation requests
type ShortenHandler struct {
	urlService service.URLService
}

// NewShortenHandler creates a new ShortenHandler instance
func NewShortenHandler(urlService service.URLService) *ShortenHandler {
	return &ShortenHandler{urlService: urlService}
}

// Shorten handles POST /api/v1/shorten
func (h *ShortenHandler) Shorten(c *gin.Context) {
	var req model.ShortenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid_request",
			"message": err.Error(),
		})
		return
	}

	resp, err := h.urlService.Shorten(c.Request.Context(), req)
	if err != nil {
		if errors.Is(err, service.ErrInvalidURL) || errors.Is(err, service.ErrInvalidAlias) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if errors.Is(err, service.ErrAliasAlreadyUsed) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "internal_server_error",
			"message": "failed to shorten URL",
		})
		return
	}

	c.JSON(http.StatusCreated, resp)
}
