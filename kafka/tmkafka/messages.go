package tmkafka

// StringMessage implements the sarama.Encoder interface
type StringMessage struct {
	Value   string
	encoded []byte
	lgth    int
}

// Encode implement sarama.Encoder return the byte encoded value
func (s *StringMessage) Encode() ([]byte, error) {
	if s.encoded != nil {
		s.lgth = len(s.encoded)
		return s.encoded, nil
	}
	s.encoded = []byte(s.Value)
	return s.encoded, nil
}

// Length implement sarama.Length return the length should return same as len(Endode())
func (s *StringMessage) Length() int {
	if s.lgth == 0 {
		// Encode not called
		_, err := s.Encode()
		if err != nil {
			return 0
		}
	}
	return s.lgth
}
