package scheduler

import "testing"

func TestBasic(t *testing.T) {
    costModel := NewSimpleCostModel()
    if costModel == nil {
        t.Error("Cost model should not be nil")
    }
}
