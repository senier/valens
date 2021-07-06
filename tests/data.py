from __future__ import annotations

import datetime

from valens.models import (
    BodyFat,
    BodyWeight,
    Exercise,
    Period,
    Routine,
    RoutineExercise,
    Sex,
    User,
    Workout,
    WorkoutSet,
)


def users_only() -> list[User]:
    return [
        User(id=1, name="Alice", sex=Sex.FEMALE),
        User(id=2, name="Bob", sex=Sex.MALE),
    ]


def users() -> list[User]:
    exercise_1 = Exercise(id=1, user_id=1, name="Exercise 1")
    exercise_2 = Exercise(id=2, user_id=2, name="Exercise 2")
    exercise_3 = Exercise(id=3, user_id=1, name="Exercise 2")
    exercise_4 = Exercise(id=4, user_id=2, name="Exercise 3")
    exercise_5 = Exercise(id=5, user_id=1, name="Unused Exercise")

    result = [
        User(
            id=1,
            name="Alice",
            sex=Sex.FEMALE,
            body_weight=[
                BodyWeight(user_id=1, date=datetime.date(2002, 2, 20), weight=67.5),
                BodyWeight(user_id=1, date=datetime.date(2002, 2, 21), weight=67.7),
                BodyWeight(user_id=1, date=datetime.date(2002, 2, 22), weight=67.3),
            ],
            body_fat=[
                BodyFat(
                    user_id=1,
                    date=datetime.date(2002, 2, 20),
                    chest=1,
                    abdominal=2,
                    tigh=3,
                    tricep=4,
                    subscapular=5,
                    suprailiac=6,
                    midaxillary=7,
                ),
                BodyFat(
                    user_id=1,
                    date=datetime.date(2002, 2, 21),
                    chest=8,
                    abdominal=9,
                    tigh=10,
                    tricep=11,
                    subscapular=12,
                    suprailiac=13,
                    midaxillary=14,
                ),
            ],
            period=[
                Period(date=datetime.date(2002, 2, 20), intensity=2),
                Period(date=datetime.date(2002, 2, 21), intensity=4),
                Period(date=datetime.date(2002, 2, 22), intensity=1),
            ],
            exercises=[
                exercise_1,
                exercise_3,
                exercise_5,
            ],
            routines=[
                Routine(
                    id=1,
                    user_id=1,
                    name="R1",
                    notes="First Routine",
                    exercises=[
                        RoutineExercise(position=1, exercise=exercise_3, sets=1),
                        RoutineExercise(position=2, exercise=exercise_1, sets=2),
                    ],
                ),
                Routine(
                    id=3,
                    user_id=1,
                    name="R2",
                    exercises=[
                        RoutineExercise(position=1, exercise=exercise_3, sets=5),
                    ],
                ),
            ],
            workouts=[
                Workout(
                    id=1,
                    user_id=1,
                    date=datetime.date(2002, 2, 20),
                    notes="First Workout",
                    sets=[
                        WorkoutSet(position=1, exercise=exercise_3, reps=10, time=4, rpe=8.0),
                        WorkoutSet(position=2, exercise=exercise_1, reps=9, time=4, rpe=8.5),
                        WorkoutSet(position=3, exercise=exercise_1, time=60, rpe=9.0),
                    ],
                ),
                Workout(
                    id=3,
                    user_id=1,
                    date=datetime.date(2002, 2, 22),
                    sets=[
                        WorkoutSet(position=1, exercise=exercise_3, reps=9),
                        WorkoutSet(position=2, exercise=exercise_3, reps=8),
                        WorkoutSet(position=3, exercise=exercise_3, reps=7),
                        WorkoutSet(position=4, exercise=exercise_3, reps=6),
                        WorkoutSet(position=5, exercise=exercise_3, reps=5),
                    ],
                ),
            ],
        ),
        User(
            id=2,
            name="Bob",
            sex=Sex.MALE,
            body_weight=[
                BodyWeight(user_id=2, date=datetime.date(2002, 2, 20), weight=82.4),
                BodyWeight(user_id=2, date=datetime.date(2002, 2, 21), weight=82.2),
                BodyWeight(user_id=2, date=datetime.date(2002, 2, 22), weight=82.6),
            ],
            body_fat=[
                BodyFat(
                    user_id=2,
                    date=datetime.date(2002, 2, 20),
                    chest=15,
                    abdominal=16,
                    tigh=17,
                    tricep=18,
                    subscapular=19,
                    suprailiac=20,
                    midaxillary=21,
                ),
                BodyFat(
                    user_id=2,
                    date=datetime.date(2002, 2, 22),
                    chest=22,
                    abdominal=23,
                    tigh=24,
                    tricep=25,
                    subscapular=26,
                    suprailiac=27,
                    midaxillary=28,
                ),
            ],
            exercises=[
                exercise_2,
                exercise_4,
            ],
            routines=[
                Routine(
                    id=2,
                    user_id=2,
                    name="R1",
                    exercises=[
                        RoutineExercise(position=1, exercise=exercise_2, sets=3),
                        RoutineExercise(position=2, exercise=exercise_4, sets=4),
                    ],
                ),
                Routine(id=4, user_id=2, name="Empty", notes="TBD"),
            ],
            workouts=[
                Workout(
                    id=2,
                    user_id=2,
                    date=datetime.date(2002, 2, 20),
                    sets=[
                        WorkoutSet(
                            position=1, exercise=exercise_2, reps=10, time=4, weight=10, rpe=8.5
                        ),
                        WorkoutSet(
                            position=2, exercise=exercise_2, reps=9, time=4, weight=8, rpe=9.0
                        ),
                        WorkoutSet(
                            position=3, exercise=exercise_2, reps=8, time=4, weight=6, rpe=9.5
                        ),
                        WorkoutSet(position=4, exercise=exercise_4, reps=7, weight=7.5),
                        WorkoutSet(position=5, exercise=exercise_4, reps=6, weight=7.5),
                        WorkoutSet(position=6, exercise=exercise_4, reps=5, weight=7.5),
                        WorkoutSet(position=7, exercise=exercise_4, reps=4, weight=7.5),
                    ],
                ),
            ],
        ),
    ]
    return result


def user(user_id: int = 1) -> User:
    return users()[user_id - 1]
