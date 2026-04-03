package fs

type resumeErrorLimitActionChoices struct{}

func (resumeErrorLimitActionChoices) Choices() []string {
	return []string{
		ResumeErrorLimitActionContinue: "continue",
		ResumeErrorLimitActionFail:     "fail",
	}
}

// ResumeErrorLimitAction controls what resume does when the distinct
// pending failed item limit is exceeded.
type ResumeErrorLimitAction = Enum[resumeErrorLimitActionChoices]

const (
	ResumeErrorLimitActionContinue ResumeErrorLimitAction = iota
	ResumeErrorLimitActionFail
)
