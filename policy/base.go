package policy

type Policy interface {
	Init()
}

type Base struct {

}

func (p Base) Init()  {

}

