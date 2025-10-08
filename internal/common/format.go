package common

import (
	"fmt"
	"strings"
)

const (
	// Default separator widths
	DefaultWidth = 80
	WideWidth    = 100
)

// PrintSeparator prints a separator line with the specified character and width
func PrintSeparator(char string, width int) {
	fmt.Println(strings.Repeat(char, width))
}

// PrintSeparatorNewline prints a separator with a newline before it
func PrintSeparatorNewline(char string, width int) {
	fmt.Println("\n" + strings.Repeat(char, width))
}

// PrintHeader prints a formatted header with title and separators
func PrintHeader(title string, width int) {
	PrintSeparatorNewline("=", width)
	fmt.Println(title)
	PrintSeparator("=", width)
}

// PrintFooter prints a formatted footer with message and separators
func PrintFooter(message string, width int) {
	PrintSeparatorNewline("=", width)
	fmt.Println(message)
	fmt.Println(strings.Repeat("=", width) + "\n")
}

// PrintBoxSeparator prints a box-drawing separator line (for sub-sections)
func PrintBoxSeparator(width int) {
	fmt.Println("├" + strings.Repeat("─", width))
}

// BoxPrefix returns the appropriate box-drawing prefix for list items
func BoxPrefix(isLast bool) string {
	if isLast {
		return "└  "
	}
	return "│  "
}

// BoxDetailPrefix returns the prefix for detail lines under list items
func BoxDetailPrefix(isLast bool) string {
	if isLast {
		return "   "
	}
	return "│  "
}
