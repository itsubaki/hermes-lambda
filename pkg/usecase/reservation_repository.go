package database

type ReservationRepository interface {
	List()
	Save()
}
