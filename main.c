#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>
#include <memory.h>
#include "mpi.h"


enum STATUS {
    RED, BLUE, EMPTY
};


enum ALLOCATION {
    ALLOCATED,
    NOT_ALLOCATED,
    INVALID
};

typedef struct {
    int row;
    int col;
    int satisfation;
    int red;
    int blue;
} InitializeMsg;


typedef struct {
    enum STATUS status;
    bool locked;
    float satisfacion;
} City;

typedef struct {
    int original_proc;
    int destination_proc;
    int x;
    int y;
    int last_edit_by;
    enum STATUS content;
    enum ALLOCATION allocation_result;
} UnHappy;

MPI_Datatype make_type_for_initialize_msg();

MPI_Datatype make_type_for_city();

MPI_Datatype make_type_for_unhappy();

void get_input_from_terminal(int *grid_size, int *red_pop, int *blue_pop, int *empty, int *satisfatcion);

void handle_input(char *arg, int *store, bool isGrid);

char decode_enum(enum STATUS status);


void send_startup_information(InitializeMsg msg, int proc,
                              MPI_Datatype mpi_initialize_message_type);

InitializeMsg create_initialize_message(int row, int col, int satisfaction, int blue, int red);

City **initialize_cache(int col);

City **initialize_grid_city(InitializeMsg initialize_struct);

UnHappy *initialize_un_happy(int size);

void default_un_happy_values(UnHappy *list, int start, int size);

void push_random_values(City **grid_city, int max_row, int col, int content_legth, enum STATUS status);

void print_grid(City **grid_city, int row, int col);

int get_process_from_offset(int offset, int row, int col);

/***
 * Ogni processo fa la richiesta con il processo adiacente e mantiene una cache cosi composta:
 *  [0] -> Riga superiore [1]-> Riga inferiore
 * @param proc : processo
 * @param grid_city : griglia
 * @param cache : struttura per memorizzare le righe adiacenti
 * @param row : numero totali di righe
 * @param col : numero totali di colonne
 * @param last_processor : numero di processori
 * @param mpi_city_type: tipo city per mpi
 */
void
do_request(int proc, City **grid_city, City **cache, int row, int col, int last_processor, MPI_Datatype mpi_city_type);

/***
 * Verifica gli adiacenti per ogni cella, esclude quelle vuote
 * @param proc processo
 * @param grid_city : griglia
 * @param cache : struttura delle righe adiacenti di proc+1 e proc-1
 * @param row numero righe
 * @param col numero colonne
 * @param satisfaction input di soddisfazione
 * @param unhappy_list vettore che colleziona l'elenco di processi insodisfatti
 * @param last_process numero processi
 * @return
 */
int
check_nearest(int proc, City **grid_city, City **cache, int row, int col, int satisfaction, UnHappy *unhappy_list,
              int last_process);

void check_satisfaction_horizontal(City **grid_city, int i, int j, int pos, int *count_near, int *satisf);

void check_satisfaction_vertical(City **grid_city, int i, int j, int pos, int *count_near, int *satisf);

void check_satisfaction_vertical_on_cache(City **grid_city, City **cache, int i, int j, int pos, int *count_near,
                                          int *satisf);

void check_satisfaction_oblique(City **grid_city, int i, int j, int pos_x, int pos_y, int *count_near, int *satisf);

void
check_satisfaction_oblique_on_cache(City **grid_city, City **cache, int i, int j, int pos_x, int pos_y, int *count_near,
                                    int *satisf);

void update_if_empty(int *count_near, int *satisf);


void resize_unhappy(UnHappy **list, int tot_unsadisfied, int local_unsadisfied, int rank);

/***
 * Esegue una reduce con un operatore custom mpi_unhappy_difference che permette
 * di identificare e fare un join di tutti gli elementi marcati
 * @param total_proc lista dei processi che hanno provato a spostarsi
 * @param unsatisfied numero insodisfatti
 * @param rank rank processo
 * @param processes numero processori
 * @param mpi_unhappy tipo mpi
 * @param mpi_unhappy_difference operatore mpi per Unhappy
 */
void
unhappy_reduce(UnHappy *total_proc, int unsatisfied, int rank, int processes,
               MPI_Datatype mpi_unhappy, MPI_Op mpi_unhappy_difference);


void difference_unhappy(UnHappy *in, UnHappy *inout, int *len, MPI_Datatype *dtype);


/***
 * Crea un unico vettore partendo dai vettori ricevuti dai processi
 * @param list_to_send: Lista di un processo x da inviare a tutti gli altri
 * @param unsatisfied: numero di insodisfatti
 * @param processes: numero di processi
 * @param mpi_unhappy: tipo mpi
 * @return Vettore unhappy che contiene tutti i vettori insodisfatti di ogni processo.
 */
UnHappy *gather_unhappy(UnHappy *list_to_send, int unsatisfied, int processes,
                        MPI_Datatype mpi_unhappy);

void search_first_empty(City **grid_city, int row, int col, int *x, int *y);

void try_to_move(UnHappy *unhappy_list, City **grid_city, int rank, int unsatisfied, int processes, int row, int col);

int update_with_empty_space(UnHappy *unhappy_list, City **grid_city, int rank, int unsatisfied, int processes);

void clear_memory(City **grid, City **cache, int row);

void unsatisfied_max_and_sum(int *unsaf, int *max, int *sum, int processes);


int main(int argc, char *argv[]) {

    int processes, rank;
    int local_unsatisfied = 0;
    int tot_unsatisfied = 0;
    int max_unsatisfied = 0;
    MPI_Op mpi_unhappy_difference;
    InitializeMsg startup_info;
    City **grid, **cache;
    UnHappy *unhappy_list;
    MPI_Status status_in_msg;
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &processes);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    srand(time(NULL) + rank);
    MPI_Datatype mpi_initialize_message_type = make_type_for_initialize_msg();
    MPI_Datatype mpi_city_type = make_type_for_city();
    MPI_Datatype mpi_unhappy = make_type_for_unhappy();
    MPI_Op_create((MPI_User_function *) difference_unhappy, false, &mpi_unhappy_difference);
    if (rank == 0) {
        int grid_size, red_pop, blue_pop, satisfaction, empty;

        if (argc < 5) {
            get_input_from_terminal(&grid_size, &red_pop, &blue_pop, &empty, &satisfaction);
        } else {
            handle_input(argv[1], &grid_size, true);
            handle_input(argv[2], &blue_pop, false);
            red_pop = 100 - blue_pop;
            handle_input(argv[4], &empty, false);
            handle_input(argv[5], &satisfaction, false);
        }
        int total_cells = grid_size * grid_size;
        int total_empty = (total_cells * empty) / 100;
        total_cells -= total_empty;
        int total_blue = (total_cells * blue_pop) / 100;
        int total_red = (total_cells * red_pop) / 100;

        //Assunzione che processes < grid_size (P<N)
        int splitted_blue = total_blue / processes;
        int splitted_red = total_red / processes;
        int splitted_row = grid_size / processes;


        int excluded = grid_size % processes;
        if (excluded != 0) {
            int assigned_processes = rand() % processes;
            int temp_proc_row = splitted_row + excluded;

            startup_info = create_initialize_message(assigned_processes == 0 ? temp_proc_row : splitted_row, grid_size,
                                                     satisfaction, splitted_blue,
                                                     splitted_red);
            for (int i = 1; i < processes; ++i) {
                send_startup_information(
                        create_initialize_message(i == assigned_processes ? temp_proc_row : splitted_row, grid_size,
                                                  satisfaction, splitted_blue,
                                                  splitted_red),
                        i,
                        mpi_initialize_message_type
                );
            }


        } else { //nessun resto nella divisione
            startup_info = create_initialize_message(splitted_row, grid_size, satisfaction, splitted_blue,
                                                     splitted_red); // for 0° process

            for (int i = 1; i < processes; ++i) {
                send_startup_information(
                        create_initialize_message(splitted_row, grid_size, satisfaction, splitted_blue, splitted_red),
                        i,
                        mpi_initialize_message_type
                );
            }
        }

    } else {
        MPI_Recv(&startup_info, 1, mpi_initialize_message_type, 0, 0, MPI_COMM_WORLD, &status_in_msg);
    }

    grid = initialize_grid_city(startup_info);
    cache = initialize_cache(startup_info.col);
    printf("rank %d - my grid %d %d \n", rank, startup_info.row, startup_info.col);
    print_grid(grid, startup_info.row, startup_info.col);
    MPI_Barrier(MPI_COMM_WORLD);
    do {
        tot_unsatisfied = 0;
        unhappy_list = initialize_un_happy(startup_info.col * startup_info.row);
        do_request(rank, grid, cache, startup_info.row, startup_info.col, processes, mpi_city_type);

        local_unsatisfied = check_nearest(rank, grid, cache, startup_info.row, startup_info.col,
                                          startup_info.satisfation, unhappy_list, processes);
        //Ottiene gli insodisfatti di ogni processo
        int unsaf[processes];
        MPI_Allgather(&local_unsatisfied, 1, MPI_INT, unsaf, 1, MPI_INT, MPI_COMM_WORLD);
        unsatisfied_max_and_sum(unsaf, &max_unsatisfied, &tot_unsatisfied, processes);
        printf("rank %d - non soddisfatti locali: %d - non soddisfatti totali %d \n", rank, local_unsatisfied,
               tot_unsatisfied);
        resize_unhappy(&unhappy_list, max_unsatisfied, local_unsatisfied, rank);

        UnHappy *unHappy_all_proc = gather_unhappy(unhappy_list, max_unsatisfied, processes, mpi_unhappy);
        MPI_Barrier(MPI_COMM_WORLD);

        do {
            try_to_move(unHappy_all_proc, grid, rank, max_unsatisfied, processes, startup_info.row, startup_info.col);
            MPI_Barrier(MPI_COMM_WORLD);
            unhappy_reduce(unHappy_all_proc, max_unsatisfied, rank, processes, mpi_unhappy, mpi_unhappy_difference);
            MPI_Barrier(MPI_COMM_WORLD);
        } while (update_with_empty_space(unHappy_all_proc, grid, rank, max_unsatisfied, processes) > 0);


        free(unhappy_list);
        free(unHappy_all_proc);
    } while (tot_unsatisfied > 0);

    printf("finito \n");
    printf("[RANK %d] - my grid %d %d \n", rank, startup_info.row, startup_info.col);
    print_grid(grid, startup_info.row, startup_info.col);


    clear_memory(grid, cache, startup_info.row);
    MPI_Type_free(&mpi_city_type);
    MPI_Type_free(&mpi_initialize_message_type);
    MPI_Type_free(&mpi_unhappy);
    MPI_Op_free(&mpi_unhappy_difference);
    MPI_Finalize();
    return 0;
}


MPI_Datatype make_type_for_initialize_msg() {
    MPI_Datatype types[5] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};
    MPI_Datatype mpi_message_type;
    MPI_Aint offsets[5];
    int blocklengths[5] = {1, 1, 1, 1, 1};
    offsets[0] = offsetof(InitializeMsg, row);
    offsets[1] = offsetof(InitializeMsg, col);
    offsets[2] = offsetof(InitializeMsg, satisfation);
    offsets[3] = offsetof(InitializeMsg, red);
    offsets[4] = offsetof(InitializeMsg, blue);

    MPI_Type_create_struct(5, blocklengths, offsets, types, &mpi_message_type);
    MPI_Type_commit(&mpi_message_type);

    return mpi_message_type;
}


MPI_Datatype make_type_for_city() {
    MPI_Datatype types[3] = {MPI_INT, MPI_C_BOOL, MPI_FLOAT};
    MPI_Datatype mpi_city;
    MPI_Aint offsets[3];
    int blocklengths[3] = {1, 1, 1};
    offsets[0] = offsetof(City, status);
    offsets[1] = offsetof(City, locked);
    offsets[2] = offsetof(City, satisfacion);

    MPI_Type_create_struct(3, blocklengths, offsets, types, &mpi_city);
    MPI_Type_commit(&mpi_city);

    return mpi_city;
}

MPI_Datatype make_type_for_unhappy() {
    MPI_Datatype types[7] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};
    MPI_Datatype mpi_unhappy;
    MPI_Aint offsets[7];
    int block_lengths[7] = {1, 1, 1, 1, 1, 1, 1};
    offsets[0] = offsetof(UnHappy, original_proc);
    offsets[1] = offsetof(UnHappy, destination_proc);
    offsets[2] = offsetof(UnHappy, x);
    offsets[3] = offsetof(UnHappy, y);
    offsets[4] = offsetof(UnHappy, last_edit_by);
    offsets[5] = offsetof(UnHappy, content);
    offsets[6] = offsetof(UnHappy, allocation_result);

    MPI_Type_create_struct(7, block_lengths, offsets, types, &mpi_unhappy);
    MPI_Type_commit(&mpi_unhappy);

    return mpi_unhappy;
}


void get_input_from_terminal(int *grid_size, int *red_pop, int *blue_pop, int *empty, int *satisfatcion) {
    char grid_string[10], blue_string[3], empty_string[3], satisfaction_string[3];
    printf("Size of grid:\n");
    scanf("%s", grid_string);
    handle_input(grid_string, grid_size, true);

    printf("blue(B) population in %% \n");
    scanf("%s", blue_string);
    handle_input(blue_string, blue_pop, false);
    *red_pop = 100 - *blue_pop;

    printf("Empty space in %% \n");
    scanf("%s", empty_string);
    handle_input(empty_string, empty, false);

    printf("Satisfatcion agent in %% \n");
    scanf("%s", satisfaction_string);
    handle_input(satisfaction_string, satisfatcion, false);
}


void handle_input(char *arg, int *store, bool isGrid) {
    char *p;
    int conv = strtol(arg, &p, 10);
    if (*p != '\0' || (isGrid && conv <= 1) || (!isGrid && (conv < 0 || conv > 100))) {
        printf("There is an error on conversion or invalid range");
        exit(-1);
    } else {
        *store = conv;
    }
}


InitializeMsg create_initialize_message(int row, int col, int satisfaction, int blue, int red) {
    InitializeMsg msg;
    msg.col = col;
    msg.satisfation = satisfaction;
    msg.blue = blue;
    msg.red = red;
    msg.row = row;
    return msg;
}

void send_startup_information(InitializeMsg msg, int proc,
                              MPI_Datatype mpi_initialize_message_type) {

    MPI_Send(&msg, 1, mpi_initialize_message_type, proc, 0, MPI_COMM_WORLD);
}


City **initialize_grid_city(InitializeMsg initialize_struct) {
    City **grid_city = (City **) malloc(initialize_struct.row * sizeof(City *));
    for (int i = 0; i < initialize_struct.row; ++i) {
        grid_city[i] = (City *) malloc(initialize_struct.col * sizeof(City));
    }
    for (int i = 0; i < initialize_struct.row; ++i) {
        for (int j = 0; j < initialize_struct.col; ++j) {
            grid_city[i][j].status = EMPTY;
            grid_city[i][j].locked = false;
            grid_city[i][j].satisfacion = 0;
        }
    }
    push_random_values(grid_city, initialize_struct.row, initialize_struct.col, initialize_struct.red, RED);
    push_random_values(grid_city, initialize_struct.row, initialize_struct.col, initialize_struct.blue, BLUE);

    return grid_city;
}

City **initialize_cache(int col) {
    City **cache = (City **) malloc(2 * sizeof(City *));
    for (int i = 0; i < 2; ++i) {
        cache[i] = (City *) malloc(col * sizeof(City));
    }
    return cache;
}

UnHappy *initialize_un_happy(int size) {
    UnHappy *list = (UnHappy *) malloc(size * sizeof(UnHappy));
    default_un_happy_values(list, 0, size);
    return list;
}

void default_un_happy_values(UnHappy *list, int start, int size) {
    for (int i = start; i < size; ++i) {
        list[i].original_proc = -1;
        list[i].destination_proc = -1;
        list[i].allocation_result = INVALID;
        list[i].content = EMPTY;
        list[i].x = -1;
        list[i].y = -1;
        list[i].last_edit_by = -1;
    }
}


void push_random_values(City **grid_city, int max_row, int max_col, int content_legth, enum STATUS status) {
    int next = content_legth;
    while (next > 0) {
        int row = rand() % max_row;
        int col = rand() % max_col;
        if (!grid_city[row][col].locked) {
            grid_city[row][col].status = status;
            grid_city[row][col].locked = (status != EMPTY);
            grid_city[row][col].satisfacion = 0;
            --next;
        }

    }

}


void
do_request(int proc, City **grid_city, City **cache, int row, int col, int last_processor,
           MPI_Datatype mpi_city_type) { //cache[0] parte alta cache[1] parte bassa
    MPI_Status status1, status2;
    if (last_processor > 1) {
        if (proc == 0) {
            MPI_Send(grid_city[row - 1], col, mpi_city_type, proc + 1, 2, MPI_COMM_WORLD);
            MPI_Recv(cache[1], col, mpi_city_type, proc + 1, 1, MPI_COMM_WORLD, &status1);
        } else if (proc == last_processor - 1) {
            MPI_Send(grid_city[0], col, mpi_city_type, proc - 1, 1, MPI_COMM_WORLD);
            MPI_Recv(cache[0], col, mpi_city_type, proc - 1, 2, MPI_COMM_WORLD, &status1);

        } else {
            MPI_Send(grid_city[0], col, mpi_city_type, proc - 1, 1, MPI_COMM_WORLD);
            MPI_Send(grid_city[row - 1], col, mpi_city_type, proc + 1, 2, MPI_COMM_WORLD);
            //ricezione
            MPI_Recv(cache[0], col, mpi_city_type, proc - 1, 2, MPI_COMM_WORLD, &status1);
            MPI_Recv(cache[1], col, mpi_city_type, proc + 1, 1, MPI_COMM_WORLD, &status2);

        }
    }


}


int
check_nearest(int proc, City **grid_city, City **cache, int row, int col, int satisfaction, UnHappy *unhappy_list,
              int last_process) {
    int unsatisfaied = 0;
    for (int i = 0; i < row; ++i) {
        for (int j = 0; j < col; ++j) {
            if (grid_city[i][j].status != EMPTY) {
                int satisf = 0;
                int count_near = 0;
                int left = j - 1;
                if (left >= 0) {
                    check_satisfaction_horizontal(grid_city, i, j, left, &count_near, &satisf);
                }
                int rigth = j + 1;
                if (rigth < col) {
                    check_satisfaction_horizontal(grid_city, i, j, rigth, &count_near, &satisf);
                }

                int top = i - 1;
                if (top >= 0) {
                    check_satisfaction_vertical(grid_city, i, j, top, &count_near, &satisf);
                } else {
                    if (proc != 0) {
                        check_satisfaction_vertical_on_cache(grid_city, cache, i, j, 0, &count_near, &satisf);
                    }
                }


                int bottom = i + 1;
                if (bottom < row) {
                    check_satisfaction_vertical(grid_city, i, j, bottom, &count_near, &satisf);
                } else {
                    if (proc != last_process - 1) {
                        check_satisfaction_vertical_on_cache(grid_city, cache, i, j, 1, &count_near, &satisf);
                    }
                }


                int no_x = i - 1;
                int no_y = j - 1;
                if (no_x >= 0 && no_y >= 0) { //se rientro nel range verifico sulla matrice locale
                    check_satisfaction_oblique(grid_city, i, j, no_x, no_y, &count_near, &satisf);
                } else {
                    if (no_y >= 0 && proc != 0) { //se non sono andato in una posizione invalida e non sono P0 (P0 sopra di esso non ha nulla) verifico in cache
                        check_satisfaction_oblique_on_cache(grid_city, cache, i, j, 0, no_y, &count_near, &satisf);
                    }
                }

                int ne_x = i - 1;
                int ne_y = j + 1;
                if (ne_x >= 0 && ne_y < col) {
                    check_satisfaction_oblique(grid_city, i, j, ne_x, ne_y, &count_near, &satisf);
                } else {
                    if (ne_y < col && proc != 0)
                        check_satisfaction_oblique_on_cache(grid_city, cache, i, j, 0, ne_y, &count_near, &satisf);
                }


                int so_x = i + 1;
                int so_y = j - 1;
                if (so_x < row && so_y >= 0) {
                    check_satisfaction_oblique(grid_city, i, j, so_x, so_y, &count_near, &satisf);
                } else {
                    if (so_y >= 0 && proc != last_process - 1)
                        check_satisfaction_oblique_on_cache(grid_city, cache, i, j, 1, so_y, &count_near, &satisf);
                }


                int se_x = i + 1;
                int se_y = j + 1;
                if (se_x < row && se_y < col) {
                    check_satisfaction_oblique(grid_city, i, j, se_x, se_y, &count_near, &satisf);
                } else {
                    if (se_y < col && proc != last_process - 1)
                        check_satisfaction_oblique_on_cache(grid_city, cache, i, j, 1, se_y, &count_near, &satisf);
                }

                float sats = (float) satisf / count_near;
                grid_city[i][j].satisfacion = sats * 100;

                if ((int) grid_city[i][j].satisfacion < satisfaction) {
                    UnHappy unHappy;
                    unHappy.content = grid_city[i][j].status;
                    unHappy.x = i;
                    unHappy.y = j;
                    unHappy.last_edit_by = -1;
                    unHappy.allocation_result = NOT_ALLOCATED;
                    unHappy.original_proc = proc;
                    unHappy.destination_proc = rand() % last_process;
                    unhappy_list[unsatisfaied++] = unHappy;
                }
            }
        }
    }
    return unsatisfaied;
}

void check_satisfaction_horizontal(City **grid_city, int i, int j, int pos, int *count_near, int *satisf) {
    if (grid_city[i][pos].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == grid_city[i][pos].status) {
            (*satisf)++;
        }
    } else {
        update_if_empty(count_near, satisf);
    }
}

void check_satisfaction_vertical(City **grid_city, int i, int j, int pos, int *count_near, int *satisf) {
    if (grid_city[pos][j].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == grid_city[pos][j].status) {
            (*satisf)++;
        }
    } else {
        update_if_empty(count_near, satisf);
    }
}

void check_satisfaction_vertical_on_cache(City **grid_city, City **cache, int i, int j, int pos, int *count_near,
                                          int *satisf) {
    if (cache[pos][j].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == cache[pos][j].status) {
            (*satisf)++;
        }
    } else {
        update_if_empty(count_near, satisf);
    }
}

void
check_satisfaction_oblique(City **grid_city, int i, int j, int pos_x, int pos_y, int *count_near, int *satisf) {
    if (grid_city[pos_x][pos_y].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == grid_city[pos_x][pos_y].status) {
            (*satisf)++;
        }
    } else {
        update_if_empty(count_near, satisf);
    }
}

void
check_satisfaction_oblique_on_cache(City **grid_city, City **cache, int i, int j, int pos_x, int pos_y,
                                    int *count_near,
                                    int *satisf) {
    if (cache[pos_x][pos_y].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == cache[pos_x][pos_y].status) {
            (*satisf)++;
        }
    } else {
        update_if_empty(count_near, satisf);
    }
}

void update_if_empty(int *count_near, int *satisf) {
    (*count_near)++;
    (*satisf)++;
}

void unsatisfied_max_and_sum(int *unsaf, int *max, int *sum, int processes) {
    *max = unsaf[0];
    *sum = 0;
    for (int i = 0; i < processes; ++i) {
        if (unsaf[i] > *max) {
            *max = unsaf[i];
        }
        *sum += unsaf[i];
    }
}


int get_process_from_offset(int offset, int row, int col) {
    int proc = -1;
    row -= 1;
    col -= 1;
    for (; offset > 0; ++proc) {
        offset -= row * col;
    }
    return proc;
}


void print_grid(City **grid_city, int row, int col) {
    for (int i = 0; i < row; ++i) {
        for (int j = 0; j < col; ++j) {
            printf("%c  ", decode_enum(grid_city[i][j].status));
        }
        printf("\n");
    }
}

char decode_enum(enum STATUS status) {
    switch (status) {
        case BLUE:
            return 'B';
        case RED:
            return 'R';
        case EMPTY:
            return 'E';
        default:
            return 'X';
    }
}

void resize_unhappy(UnHappy **list, int tot_unsadisfied, int local_unsadisfied, int rank) {

    *list = realloc(*list, tot_unsadisfied * sizeof(UnHappy));
    default_un_happy_values(*list, local_unsadisfied, tot_unsadisfied);
}


void
unhappy_reduce(UnHappy *total_proc, int unsatisfied, int rank, int processes,
               MPI_Datatype mpi_unhappy, MPI_Op mpi_unhappy_difference) {

    int size = unsatisfied * processes;
    UnHappy *temp = (UnHappy *) malloc(sizeof(UnHappy) * size);
    memcpy(temp, total_proc, sizeof(UnHappy) * size);
    MPI_Allreduce(temp, total_proc, size, mpi_unhappy, mpi_unhappy_difference, MPI_COMM_WORLD);
    //reset marker
    for (int j = 0; j < size; ++j) {
        if (total_proc[j].last_edit_by != -1)
            total_proc[j].last_edit_by = -1;
    }

    free(temp);
}


void difference_unhappy(UnHappy *in, UnHappy *inout, int *len, MPI_Datatype *dtype) {
    for (int i = 0; i < *len; ++i) {
        if (in[i].last_edit_by != -1 && in[i].allocation_result != INVALID) {
            inout[i].allocation_result = in[i].allocation_result;
            inout[i].destination_proc = in[i].destination_proc;
            inout[i].last_edit_by = in[i].last_edit_by;
        }
    }
}


UnHappy *gather_unhappy(UnHappy *list_to_send, int unsatisfied, int processes,
                        MPI_Datatype mpi_unhappy) {
    int size = unsatisfied * processes;
    UnHappy *unHappy_total_proc = (UnHappy *) malloc(sizeof(UnHappy) * size);

    MPI_Allgather(list_to_send, unsatisfied, mpi_unhappy, unHappy_total_proc, unsatisfied, mpi_unhappy, MPI_COMM_WORLD);

    return unHappy_total_proc;
}


void
try_to_move(UnHappy *unhappy_list, City **grid_city, int rank, int unsatisfied, int processes, int row, int col) {
    int size = unsatisfied * processes;
    for (int i = 0; i < size; ++i) {
        //quelli invalidi è padding
        if (unhappy_list[i].destination_proc == rank && unhappy_list[i].allocation_result == NOT_ALLOCATED) {
            int nx, ny;
            search_first_empty(grid_city, row, col, &nx, &ny);
            if (nx != -1 && ny != -1) {
                grid_city[nx][ny].status = unhappy_list[i].content;
                grid_city[nx][ny].locked = true;
                grid_city[nx][ny].satisfacion = 0;
                unhappy_list[i].allocation_result = ALLOCATED;
                unhappy_list[i].last_edit_by = rank;
            } else {
                int new_proc = 0;
                do {
                    new_proc = rand() % processes;
                } while (new_proc == rank);
                unhappy_list[i].destination_proc = new_proc;
                unhappy_list[i].last_edit_by = rank;
            }
        }

    }
}

void search_first_empty(City **grid_city, int row, int col, int *x, int *y) {
    for (int i = 0; i < row; ++i) {
        for (int j = 0; j < col; ++j) {
            if (!grid_city[i][j].locked) {
                *x = i;
                *y = j;
                return;
            }
        }
        *x = -1;
        *y = -1;
    }
}


int update_with_empty_space(UnHappy *unhappy_list, City **grid_city, int rank, int unsatisfied, int processes) {
    int size = unsatisfied * processes;
    int not_allocated = 0;
    for (int i = 0; i < size; ++i) {
        if (unhappy_list[i].allocation_result == ALLOCATED && unhappy_list[i].original_proc == rank) {
            int x = 0;
            int j = 0;
            x = unhappy_list[i].x;
            j = unhappy_list[i].y;
            grid_city[x][j].status = EMPTY;
            grid_city[x][j].locked = false;
            grid_city[x][j].satisfacion = 0;
        } else if (unhappy_list[i].allocation_result == NOT_ALLOCATED) {
            ++not_allocated;
        }
    }
    return not_allocated;
}


void clear_memory(City **grid, City **cache, int row) {
    for (int i = 0; i < row; ++i) {
        free(grid[i]);
    }
    for (int i = 0; i < 2; ++i) {
        free(cache[i]);
    }


}